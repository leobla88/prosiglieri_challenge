-- =====================================================================
-- Representative BigQuery DDL for the eCommerce data platform.
--
-- This is the production target dialect the local dbt/DuckDB project
-- (dbt/) mirrors for portability. It is not meant to be run standalone --
-- see design_process.md ("Why DuckDB locally") for why the working repo
-- builds on DuckDB while this file documents the BigQuery equivalent:
-- partitioning, clustering, and a native MERGE-based incremental load,
-- none of which DuckDB's local project needs to demonstrate the same
-- logical pattern.
--
-- Naming matches the dbt models 1:1: staging -> intermediate -> marts.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS `project.raw`
  OPTIONS (description = 'Landed, 1:1 copies of source tables. Never queried directly by BI.');

CREATE SCHEMA IF NOT EXISTS `project.marts`
  OPTIONS (description = 'Business-ready star schema. The only schema BI tools connect to.');

-- ---------------------------------------------------------------------
-- 1. RAW LAYER — one table per source, plus the _loaded_at watermark the
--    EL layer stamps on landing (Datastream CDC for Sales/Product DB,
--    a Composer DAG for the FX API — see design_process.md architecture).
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `project.raw.orders` (
    id            INT64      NOT NULL,
    customer_id   INT64      NOT NULL,
    order_date    TIMESTAMP  NOT NULL,
    total_amount  NUMERIC(10, 2) NOT NULL,
    currency      STRING     NOT NULL,   -- not validated at landing time: source emits non-ISO codes (see stg_orders)
    status        STRING     NOT NULL,
    valid_from    TIMESTAMP  NOT NULL,   -- forwarded from the source's SQL Server temporal columns
    valid_to      TIMESTAMP  NOT NULL,
    _loaded_at    TIMESTAMP  NOT NULL    -- EL watermark; drives the incremental load below
)
PARTITION BY DATE(order_date)
CLUSTER BY customer_id;

CREATE TABLE IF NOT EXISTS `project.raw.order_items` (
    id          INT64      NOT NULL,
    order_id    INT64      NOT NULL,
    product_id  INT64      NOT NULL,     -- intentionally not a FK -- mirrors the upstream system (orphans expected)
    quantity    INT64      NOT NULL,
    unit_price  NUMERIC(10, 2) NOT NULL,
    currency    STRING     NOT NULL,
    _loaded_at  TIMESTAMP  NOT NULL
)
CLUSTER BY order_id;

CREATE TABLE IF NOT EXISTS `project.raw.customers` (
    id                 INT64     NOT NULL,
    name               STRING    NOT NULL,
    email              STRING    NOT NULL,
    registration_date  TIMESTAMP NOT NULL,
    country            STRING    NOT NULL
);

CREATE TABLE IF NOT EXISTS `project.raw.product_descriptions` (
    id          INT64      NOT NULL,
    name        STRING     NOT NULL,
    category    STRING     NOT NULL,
    description STRING,
    base_price  NUMERIC(10, 2) NOT NULL,
    currency    STRING     NOT NULL,
    valid_from  TIMESTAMP  NOT NULL,     -- SQL Server system-versioning history, landed as-is
    valid_to    TIMESTAMP  NOT NULL
);

CREATE TABLE IF NOT EXISTS `project.raw.fx_rates` (
    base_currency  STRING    NOT NULL,
    quote_currency STRING    NOT NULL,
    rate_date      DATE      NOT NULL,
    rate           NUMERIC(18, 6) NOT NULL
)
PARTITION BY rate_date;

-- ---------------------------------------------------------------------
-- 2. MARTS LAYER — the star schema. Matches dbt/models/marts/*.sql.
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `project.marts.dim_customers` (
    customer_id       INT64     NOT NULL,
    customer_name     STRING    NOT NULL,
    email             STRING    NOT NULL,
    registration_date TIMESTAMP NOT NULL,
    country           STRING    NOT NULL
)
CLUSTER BY customer_id;

-- Type-2 SCD. In production this is hydrated straight from
-- raw.product_descriptions' own valid_from/valid_to history (the source
-- already system-versions itself), not re-derived by diffing dbt runs --
-- see "Ingestion & transformation strategy" in design_process.md.
CREATE TABLE IF NOT EXISTS `project.marts.dim_products` (
    product_id    INT64      NOT NULL,  -- -1 = "Unknown / Unmatched Product" placeholder row
    product_name  STRING     NOT NULL,
    category      STRING     NOT NULL,
    description   STRING,
    base_price    NUMERIC(10, 2),
    currency      STRING,
    valid_from    TIMESTAMP,
    valid_to      TIMESTAMP,            -- NULL = current version
    is_current    BOOL       NOT NULL
)
CLUSTER BY product_id;

CREATE TABLE IF NOT EXISTS `project.marts.dim_date` (
    date_day          DATE    NOT NULL,
    year              INT64   NOT NULL,
    quarter           INT64   NOT NULL,
    month             INT64   NOT NULL,
    month_name        STRING  NOT NULL,
    day_of_month      INT64   NOT NULL,
    day_of_week_num   INT64   NOT NULL,
    day_of_week_name  STRING  NOT NULL,
    is_weekend        BOOL    NOT NULL
);

CREATE TABLE IF NOT EXISTS `project.marts.dim_time_of_day` (
    hour_of_day    INT64   NOT NULL,   -- 0-23
    day_part       STRING  NOT NULL,   -- Night / Morning / Afternoon / Evening
    day_part_sort  INT64   NOT NULL
);

-- Grain: one row per order line item. Partitioned + clustered for the two
-- business questions' access patterns (date-range scans, product/customer
-- rollups) at assumed production scale (millions of rows).
CREATE TABLE IF NOT EXISTS `project.marts.fact_order_items` (
    order_item_id       INT64      NOT NULL,
    order_id            INT64      NOT NULL,
    customer_id         INT64      NOT NULL,
    product_id          INT64      NOT NULL,   -- -1 when order_items.product_id has no catalogue match
    order_date          TIMESTAMP  NOT NULL,
    order_date_day      DATE       NOT NULL,
    order_hour          INT64      NOT NULL,
    quantity            INT64      NOT NULL,
    unit_price          NUMERIC(10, 2) NOT NULL,
    currency            STRING     NOT NULL,
    is_valid_currency   BOOL       NOT NULL,
    rate_to_usd         NUMERIC(18, 6),
    line_amount_native  NUMERIC(12, 2) NOT NULL,
    line_amount_usd     NUMERIC(12, 2),
    is_fx_convertible   BOOL       NOT NULL,
    is_unknown_product  BOOL       NOT NULL,
    _loaded_at          TIMESTAMP  NOT NULL
)
PARTITION BY order_date_day
CLUSTER BY product_id, customer_id;

-- ---------------------------------------------------------------------
-- 3. TRANSFORMATION OBJECT — FX-normalization view.
--    Equivalent of dbt/models/intermediate/int_order_items_usd.sql.
--    "As-of" join: takes the closest available FX rate on or before
--    order_date for that currency, so gaps in FX history (weekends,
--    provider outages) degrade gracefully instead of failing the join.
-- ---------------------------------------------------------------------

CREATE OR REPLACE VIEW `project.marts.int_order_items_usd_v` AS
WITH items AS (
    SELECT
        oi.id AS order_item_id, oi.order_id, oi.product_id, oi.quantity, oi.unit_price,
        UPPER(TRIM(oi.currency)) AS currency,
        oi.quantity * oi.unit_price AS line_amount_native,
        o.order_date, oi._loaded_at
    FROM `project.raw.order_items` oi
    JOIN `project.raw.orders` o ON o.id = oi.order_id
),
rate_candidates AS (
    SELECT
        i.order_item_id,
        f.rate AS rate_to_usd,
        ABS(DATE_DIFF(DATE(i.order_date), f.rate_date, DAY)) AS days_from_order,
        IF(f.rate_date <= DATE(i.order_date), 0, 1) AS is_future_rate
    FROM items i
    JOIN `project.raw.fx_rates` f
      ON f.base_currency = i.currency AND f.quote_currency = 'USD'
),
best_rate AS (
    SELECT order_item_id, rate_to_usd
    FROM (
        SELECT order_item_id, rate_to_usd,
               ROW_NUMBER() OVER (PARTITION BY order_item_id ORDER BY is_future_rate, days_from_order) AS rn
        FROM rate_candidates
    )
    WHERE rn = 1
)
SELECT
    i.*,
    br.rate_to_usd,
    br.rate_to_usd * i.line_amount_native AS line_amount_usd,
    br.rate_to_usd IS NOT NULL           AS is_fx_convertible
FROM items i
LEFT JOIN best_rate br USING (order_item_id);

-- ---------------------------------------------------------------------
-- 4. INCREMENTAL LOAD PATTERN — MERGE into fact_order_items.
--    Equivalent of dbt's `incremental_strategy='merge'` (the local
--    DuckDB build uses delete+insert; see fact_order_items.sql for why).
--    Scheduled by Composer right after the dbt run that refreshes
--    dim_products / dim_customers, on the same _loaded_at watermark
--    the EL layer stamped at landing time.
-- ---------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE `project.marts.sp_load_fact_order_items`()
BEGIN
    DECLARE last_watermark TIMESTAMP DEFAULT (
        SELECT COALESCE(MAX(_loaded_at), TIMESTAMP '1900-01-01') FROM `project.marts.fact_order_items`
    );

    MERGE `project.marts.fact_order_items` AS target
    USING (
        SELECT
            u.order_item_id,
            u.order_id,
            o.customer_id,
            COALESCE(p.product_id, -1) AS product_id,
            u.order_date,
            DATE(u.order_date)         AS order_date_day,
            EXTRACT(HOUR FROM u.order_date) AS order_hour,
            u.quantity,
            u.unit_price,
            u.currency,
            u.currency IN ('USD', 'EUR', 'GBP') AS is_valid_currency,
            u.rate_to_usd,
            u.line_amount_native,
            u.line_amount_usd,
            u.is_fx_convertible,
            p.product_id IS NULL        AS is_unknown_product,
            u._loaded_at
        FROM `project.marts.int_order_items_usd_v` u
        JOIN `project.raw.orders` o ON o.id = u.order_id
        LEFT JOIN `project.marts.dim_products` p ON p.product_id = u.product_id
        WHERE u._loaded_at > last_watermark          -- only the delta since the last successful run
    ) AS delta
    ON target.order_item_id = delta.order_item_id
    WHEN MATCHED THEN UPDATE SET
        quantity = delta.quantity,
        unit_price = delta.unit_price,
        line_amount_usd = delta.line_amount_usd,
        is_fx_convertible = delta.is_fx_convertible,
        _loaded_at = delta._loaded_at
    WHEN NOT MATCHED THEN
        INSERT (order_item_id, order_id, customer_id, product_id, order_date, order_date_day, order_hour,
                quantity, unit_price, currency, is_valid_currency, rate_to_usd, line_amount_native,
                line_amount_usd, is_fx_convertible, is_unknown_product, _loaded_at)
        VALUES (delta.order_item_id, delta.order_id, delta.customer_id, delta.product_id, delta.order_date,
                delta.order_date_day, delta.order_hour, delta.quantity, delta.unit_price, delta.currency,
                delta.is_valid_currency, delta.rate_to_usd, delta.line_amount_native, delta.line_amount_usd,
                delta.is_fx_convertible, delta.is_unknown_product, delta._loaded_at);
END;

-- ---------------------------------------------------------------------
-- 5. BUSINESS-QUESTION QUERIES — same logic as
--    dbt/models/reporting/rpt_top_products.sql and rpt_promo_time_of_day.sql,
--    included here so this file stands on its own as requested.
-- ---------------------------------------------------------------------

-- Q1: top performers by volume and revenue (excludes the -1 placeholder
-- product and non-FX-convertible lines -- see design_process.md).
SELECT
    p.product_id, p.product_name, p.category,
    SUM(f.quantity) AS units_sold,
    SUM(IF(f.is_fx_convertible, f.line_amount_usd, NULL)) AS revenue_usd,
    RANK() OVER (ORDER BY SUM(f.quantity) DESC) AS rank_by_volume,
    RANK() OVER (ORDER BY SUM(IF(f.is_fx_convertible, f.line_amount_usd, NULL)) DESC) AS rank_by_revenue
FROM `project.marts.fact_order_items` f
JOIN `project.marts.dim_products` p ON p.product_id = f.product_id
WHERE p.product_id != -1
GROUP BY 1, 2, 3
ORDER BY revenue_usd DESC;

-- Q2: optimal time of day for promotions.
SELECT
    t.hour_of_day, t.day_part,
    COUNT(DISTINCT f.order_id) AS order_count,
    SUM(IF(f.is_fx_convertible, f.line_amount_usd, NULL)) AS revenue_usd,
    RANK() OVER (ORDER BY COUNT(DISTINCT f.order_id) DESC) AS rank_by_order_count
FROM `project.marts.dim_time_of_day` t
LEFT JOIN `project.marts.fact_order_items` f ON f.order_hour = t.hour_of_day
GROUP BY 1, 2
ORDER BY t.hour_of_day;
