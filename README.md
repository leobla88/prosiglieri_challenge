# eCommerce Data Platform — Senior Data Engineer Challenge

Design-focused challenge submission (Data Warehouse & Pipeline Design for eCommerce Analytics). The main
deliverable is **[design_process.md](design_process.md)** — architecture, dimensional model, ingestion
strategy, data-quality handling, DevOps approach, and the two business-question answers with real
numbers. This README only covers how to run the project locally.

## What's here

| Path | What |
|---|---|
| [`design_process.md`](design_process.md) | Main design document — read this first. |
| [`diagrams/`](diagrams) | Architecture diagram + dimensional model (star schema) ERD. |
| [`sql/ddl_bigquery.sql`](sql/ddl_bigquery.sql) | Standalone BigQuery-dialect DDL: raw + marts schemas, an FX-normalization view, an incremental `MERGE` procedure, and the two business-question queries. |
| [`dbt/`](dbt) | A working dbt project (staging → intermediate → marts → reporting), runnable locally against DuckDB. |
| [`bi_mockup/`](bi_mockup) | Looker Studio wireframe, built from real numbers queried out of the local dbt build. |
| [`scripts/`](scripts) | Everything used to build the above: seed extraction, diagrams, BI mockup. |
| [`reference/`](reference) | Clone of the challenge's own repo (source SQL, sample FX rates) — not part of the submission, kept for traceability. |

## Why this runs on DuckDB, not BigQuery

See "[Why DuckDB locally](design_process.md#why-duckdb-locally)" in the design doc — short version: this
lets `git clone && dbt build` work with no GCP account or credentials, while `sql/ddl_bigquery.sql`
carries the literal BigQuery-dialect equivalent (partitioning, clustering, native `MERGE`).

## Running it locally

Requires Python 3.11+ (tested on 3.14) and no other dependencies.

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install dbt-core dbt-duckdb duckdb pandas matplotlib

# 1. Regenerate the CSV seeds from the source SQL (already committed under dbt/seeds/,
#    but this is how they were produced from reference/data_engineer_assets/):
python scripts/extract_seeds.py

# 2. Build the warehouse
cd dbt
dbt seed
dbt run --select staging          # staging views must exist before the snapshot can query them
dbt snapshot                      # SCD2 for products
dbt run                           # intermediate + marts + reporting
dbt test

# 3. (from repo root) rebuild the diagrams / BI mockup from the live warehouse
cd ..
python scripts/build_architecture_diagram.py
python scripts/build_dimensional_model_diagram.py
python scripts/build_bi_mockup.py
```

All `dbt` commands run from the `dbt/` directory, which holds both `dbt_project.yml` and `profiles.yml`
(pointing at a local `dbt/warehouse.duckdb` file, gitignored).

### Incremental load demo

Proves the incremental strategy in `fact_order_items` actually only processes new rows — see
["Ingestion & transformation strategy"](design_process.md#ingestion--transformation-strategy) for why
this matters and how the watermark is constructed:

```bash
cd dbt
dbt seed
dbt run --select staging --vars '{"orders_seed": "raw_orders_batch1", "order_items_seed": "raw_order_items_batch1"}'
dbt snapshot
dbt run --vars '{"orders_seed": "raw_orders_batch1", "order_items_seed": "raw_order_items_batch1"}'
# -> fact_order_items has 303 rows (the initial "batch 1" load)

dbt run
# -> fact_order_items has 363 rows: exactly the 60 new "batch 2" order_items were added,
#    the original 303 were not reprocessed, and there are zero duplicate order_item_ids.
```

### Useful queries against the local warehouse

```bash
python -c "
import duckdb
con = duckdb.connect('dbt/warehouse.duckdb', read_only=True)
print(con.execute('select * from main_reporting.rpt_top_products order by revenue_usd desc limit 10').df())
print(con.execute('select * from main_reporting.rpt_promo_time_of_day order by hour_of_day').df())
print(con.execute('select * from main_reporting.rpt_data_quality_exceptions limit 20').df())
"
```

## Data source

`dbt/seeds/raw_*.csv` are parsed directly from the challenge's own upstream fixtures
(`reference/data_engineer_assets/source_sales.sql`, `source_products.sql`, `sample_fx_rates.json`) by
`scripts/extract_seeds.py` — real operational data with real inconsistencies (non-ISO currency codes,
orphaned `product_id`s), not a cleaned-up synthetic dataset. See
["Data quality"](design_process.md#data-quality) for how the pipeline handles both.
