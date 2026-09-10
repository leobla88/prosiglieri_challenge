{#
  Grain: one row per order line item.

  Incremental strategy: filters on _loaded_at, a watermark stamped by the EL
  layer when a row lands in the raw zone (see scripts/extract_seeds.py and
  "Ingestion & transformation strategy" in design_process.md). Locally this
  uses delete+insert keyed on order_item_id, which is idempotent and safe to
  re-run; on BigQuery the same pattern becomes
  `incremental_strategy='merge'` with a native MERGE on order_item_id (see
  sql/ddl_bigquery.sql) -- functionally equivalent, just pushed down natively.

  Product join: keys to dim_products on the natural key only. In production,
  once the source's temporal history (or a dbt-snapshot built over several real
  runs) has more than one version per product, this would instead resolve the
  dimension row valid at order_date (valid_from <= order_date < valid_to) so
  each sale is attributed to the price/category that was actually active at
  sale time -- the single-snapshot local dataset has no such history yet to
  demonstrate against.
#}

{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
    )
}}

with items as (
    select * from {{ ref('int_order_items_usd') }}
    {% if is_incremental() %}
    where _loaded_at > (select coalesce(max(_loaded_at), timestamp '1900-01-01') from {{ this }})
    {% endif %}
),

orders as (
    select order_id, customer_id from {{ ref('stg_orders') }}
),

products as (
    select product_id from {{ ref('dim_products') }}
)

select
    i.order_item_id,
    i.order_id,
    o.customer_id,
    coalesce(p.product_id, -1)         as product_id,
    i.order_date,
    cast(i.order_date as date)         as order_date_day,
    extract(hour from i.order_date)    as order_hour,
    i.quantity,
    i.unit_price,
    i.currency,
    i.is_valid_currency,
    i.line_amount_native,
    i.rate_to_usd,
    i.line_amount_usd,
    i.is_fx_convertible,
    (p.product_id is null)             as is_unknown_product,
    i._loaded_at
from items i
join orders o on i.order_id = o.order_id
left join products p on p.product_id = i.product_id
