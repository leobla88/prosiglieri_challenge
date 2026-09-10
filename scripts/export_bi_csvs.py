"""
Exports the reporting/marts tables Looker Studio needs for the BI mockup as
CSVs, so they can be uploaded to Google Sheets (or Looker Studio's "File
Upload" connector) — Looker Studio has no native DuckDB connector, so this
is the hand-off point from the local warehouse to a tool that does.
"""
import duckdb
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "dbt" / "warehouse.duckdb"
OUT_DIR = ROOT / "bi_mockup" / "csv_export"
OUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(DB), read_only=True)

exports = {
    "rpt_top_products": """
        select product_id, product_name, category, units_sold, revenue_usd,
               rank_by_volume, rank_by_revenue
        from main_reporting.rpt_top_products
        order by revenue_usd desc
    """,
    "rpt_promo_time_of_day": """
        select hour_of_day, day_part, order_count, revenue_usd
        from main_reporting.rpt_promo_time_of_day
        order by hour_of_day
    """,
    "rpt_data_quality_exceptions": """
        select order_item_id, order_id, order_date, product_id, currency,
               line_amount_native, flag_invalid_currency_code,
               flag_product_id_not_in_catalogue, exception_reasons
        from main_reporting.rpt_data_quality_exceptions
        order by order_date
    """,
    "kpis_summary": """
        select
            (select round(sum(line_amount_usd), 2) from main_marts.fact_order_items where is_fx_convertible) as total_revenue_usd,
            (select sum(quantity) from main_marts.fact_order_items) as total_units,
            (select count(distinct order_id) from main_marts.fact_order_items) as total_orders,
            (select count(*) from main_reporting.rpt_data_quality_exceptions) as dq_exceptions,
            (select count(*) from main_marts.fact_order_items) as total_lines
    """,
}

for name, query in exports.items():
    df = con.execute(query).df()
    out_path = OUT_DIR / f"{name}.csv"
    df.to_csv(out_path, index=False)
    print(f"wrote {out_path} ({len(df)} rows)")

con.close()
