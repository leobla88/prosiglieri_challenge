"""
Parses the upstream SQL Server source dumps (reference/data_engineer_assets/*.sql
and sample_fx_rates.json) into flat CSV seeds that stand in for a raw extract
landed from the operational databases.

IDENTITY columns (customers.id, orders.id, order_items.id) are not present in
the INSERT statements, so ids are assigned by insertion order -- this matches
how order_items.order_id references orders (1-based, in source order).

Also emits an "as of" snapshot split of orders/order_items (first 400 orders)
used to demo the incremental load: seed the _batch1 files first, dbt build,
then reseed with the full files and dbt build again to show only the delta
gets processed.
"""
import csv
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SALES_SQL = ROOT / "reference" / "data_engineer_assets" / "source_sales.sql"
PRODUCTS_SQL = ROOT / "reference" / "data_engineer_assets" / "source_products.sql"
FX_JSON = ROOT / "reference" / "data_engineer_assets" / "sample_fx_rates.json"
SEEDS_DIR = ROOT / "dbt" / "seeds"

TUPLE_RE = re.compile(r"\(((?:[^()']|'(?:[^']|'')*')*)\)")

# Simulated EL-tool watermark timestamps: batch 1 stands in for an initial
# historical backfill, batch 2 for the next nightly incremental run. These are
# baked into the seed data itself (like a real EL tool's _extracted_at column)
# so the demo's watermark does not depend on wall-clock time when it is run.
LOADED_AT_BATCH1 = "2025-01-01 02:00:00"
LOADED_AT_BATCH2 = "2025-01-02 02:00:00"


def split_tuple(inner: str):
    """Split one VALUES (...) tuple into fields, respecting quoted strings
    (including doubled '' escapes) and commas."""
    fields, buf, in_str, i = [], "", False, 0
    while i < len(inner):
        ch = inner[i]
        if in_str:
            if ch == "'" and inner[i : i + 2] == "''":
                buf += "'"
                i += 2
                continue
            if ch == "'":
                in_str = False
                i += 1
                continue
            buf += ch
        else:
            if ch == "'":
                in_str = True
                i += 1
                continue
            if ch == ",":
                fields.append(buf.strip())
                buf = ""
                i += 1
                continue
            buf += ch
        i += 1
    fields.append(buf.strip())
    return [f[1:-1] if f.startswith("'") and f.endswith("'") else f for f in fields]


def extract_values_block(sql: str, insert_marker: str) -> list[list[str]]:
    start = sql.index(insert_marker) + len(insert_marker)
    # block runs until the next blank-line-terminated statement ends with ';'
    end = sql.index(";", start)
    block = sql[start:end]
    return [split_tuple(m.group(1)) for m in TUPLE_RE.finditer(block)]


def write_csv(path: Path, header: list[str], rows: list[list]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"wrote {path.relative_to(ROOT)} ({len(rows)} rows)")


def main():
    sales_sql = SALES_SQL.read_text(encoding="utf-8")
    products_sql = PRODUCTS_SQL.read_text(encoding="utf-8")
    fx = json.loads(FX_JSON.read_text(encoding="utf-8"))

    # --- customers (IDENTITY id = 1-based insertion order) ---
    customers_raw = extract_values_block(sales_sql, "INSERT INTO customers (name, email, registration_date, country) VALUES")
    customers = [[i + 1, *row] for i, row in enumerate(customers_raw)]
    write_csv(
        SEEDS_DIR / "raw_customers.csv",
        ["id", "name", "email", "registration_date", "country"],
        customers,
    )

    # --- orders (IDENTITY id = 1-based insertion order) ---
    # cutoff below defines which orders were "already loaded" (batch 1) vs.
    # "arrive in the next incremental run" (batch 2) -- see cutoff comment below.
    cutoff = 300
    orders_raw = extract_values_block(
        sales_sql, "INSERT INTO orders (customer_id, order_date, total_amount, currency, status) VALUES"
    )
    orders = [
        [i + 1, *row, LOADED_AT_BATCH1 if (i + 1) <= cutoff else LOADED_AT_BATCH2]
        for i, row in enumerate(orders_raw)
    ]
    orders_header = ["id", "customer_id", "order_date", "total_amount", "currency", "status", "_loaded_at"]
    write_csv(SEEDS_DIR / "raw_orders.csv", orders_header, orders)

    # --- order_items (IDENTITY id = 1-based insertion order; order_id/product_id as-is) ---
    items_raw = extract_values_block(
        sales_sql, "INSERT INTO order_items (order_id, product_id, quantity, unit_price, currency) VALUES"
    )
    items = [
        [i + 1, *row, LOADED_AT_BATCH1 if int(row[0]) <= cutoff else LOADED_AT_BATCH2]
        for i, row in enumerate(items_raw)
    ]
    items_header = ["id", "order_id", "product_id", "quantity", "unit_price", "currency", "_loaded_at"]
    write_csv(SEEDS_DIR / "raw_order_items.csv", items_header, items)

    # --- products ---
    products_raw = extract_values_block(
        products_sql,
        "INSERT INTO product_descriptions (id, name, category, description, base_price, currency) VALUES",
    )
    write_csv(
        SEEDS_DIR / "raw_products.csv",
        ["id", "name", "category", "description", "base_price", "currency"],
        products_raw,
    )

    # --- fx rates (flatten base/date/rates responses to base,quote,date,rate) ---
    fx_rows = []
    for resp in fx["responses"]:
        for quote_ccy, rate in resp["rates"].items():
            fx_rows.append([resp["base"], quote_ccy, resp["date"], rate])
    write_csv(SEEDS_DIR / "raw_fx_rates.csv", ["base_currency", "quote_currency", "rate_date", "rate"], fx_rows)

    # --- incremental-demo batch split: orders 1-300 = "batch 1" (initial load).
    # order_items only cover orders 1-360 (orders 361-453 arrive with no line items
    # yet in this sample -- a real data-quality wrinkle called out in design_process.md),
    # so cutoff=300 keeps both orders AND order_items non-trivial in batch 2.
    batch1_orders = [r for r in orders if r[0] <= cutoff]
    batch1_order_ids = {r[0] for r in batch1_orders}
    batch1_items = [r for r in items if int(r[1]) in batch1_order_ids]
    write_csv(SEEDS_DIR / "raw_orders_batch1.csv", orders_header, batch1_orders)
    write_csv(SEEDS_DIR / "raw_order_items_batch1.csv", items_header, batch1_items)

    print(f"\ntotal customers={len(customers)} orders={len(orders)} order_items={len(items)} products={len(products_raw)}")
    print(f"batch1 (initial load) orders={len(batch1_orders)} order_items={len(batch1_items)}")
    print(f"batch2 (incremental delta) orders={len(orders) - len(batch1_orders)} order_items={len(items) - len(batch1_items)}")


if __name__ == "__main__":
    main()
