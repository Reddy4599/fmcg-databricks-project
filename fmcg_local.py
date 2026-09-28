"""Reproducible local FMCG pipeline and dashboard (Python standard library only).

The archived Databricks notebooks remain in the repository. This entry point makes
the same business flow runnable without access to their private S3 bucket.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sqlite3
import tempfile
from contextlib import closing
from decimal import Decimal, InvalidOperation
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
DEFAULT_DATA = ROOT / "sample_data"
DEFAULT_DB = ROOT / "local" / "fmcg.sqlite"
CITY_FIXES = {
    "bengaluruu": "Bengaluru", "bengalore": "Bengaluru",
    "hyderabadd": "Hyderabad", "hyderbad": "Hyderabad",
    "newdelhi": "New Delhi", "newdheli": "New Delhi",
    "newdelhee": "New Delhi",
}


def read_csv(path: Path, columns: set[str]) -> list[dict[str, str]]:
    if not path.is_file():
        raise FileNotFoundError(f"Missing source file: {path}")
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        missing = columns - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path.name} is missing columns: {', '.join(sorted(missing))}")
        return list(reader)


def iso_date(value: str) -> str:
    value = value.strip()
    for pattern in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y", "%A, %B %d, %Y"):
        try:
            return dt.datetime.strptime(value, pattern).date().isoformat()
        except ValueError:
            pass
    raise ValueError(f"invalid date {value!r}")


def cents(value: str) -> int:
    try:
        amount = Decimal(value.strip())
        if not amount.is_finite() or amount <= 0 or amount.as_tuple().exponent < -2:
            raise ValueError
        return int(amount * 100)
    except (InvalidOperation, ValueError):
        raise ValueError(f"invalid positive price {value!r}") from None


def schema(db: sqlite3.Connection) -> None:
    db.executescript("""
        PRAGMA foreign_keys = ON;
        CREATE TABLE bronze_customers (customer_id TEXT, customer_name TEXT, city TEXT, market TEXT, platform TEXT, channel TEXT);
        CREATE TABLE bronze_products (product_id TEXT, product_name TEXT, category TEXT);
        CREATE TABLE bronze_prices (product_id TEXT, year TEXT, price_inr TEXT);
        CREATE TABLE bronze_orders (source_file TEXT, order_id TEXT, customer_id TEXT, product_id TEXT, order_qty TEXT, order_placement_date TEXT);
        CREATE TABLE silver_customers (customer_id TEXT PRIMARY KEY, customer_name TEXT NOT NULL, city TEXT NOT NULL, market TEXT NOT NULL, platform TEXT NOT NULL, channel TEXT NOT NULL);
        CREATE TABLE silver_products (product_id TEXT PRIMARY KEY, product_name TEXT NOT NULL, category TEXT NOT NULL);
        CREATE TABLE silver_prices (product_id TEXT NOT NULL, year INTEGER NOT NULL, price_cents INTEGER NOT NULL, PRIMARY KEY (product_id, year));
        CREATE TABLE silver_orders (order_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, product_id TEXT NOT NULL, order_qty INTEGER NOT NULL, order_date TEXT NOT NULL);
        CREATE TABLE rejected_rows (source_file TEXT NOT NULL, record_id TEXT, reason TEXT NOT NULL);
        CREATE TABLE pipeline_batch_audit (source_file TEXT PRIMARY KEY, records_read INTEGER NOT NULL, records_accepted INTEGER NOT NULL, records_rejected INTEGER NOT NULL);
        CREATE TABLE gold_sales (order_id TEXT PRIMARY KEY, order_date TEXT NOT NULL, year INTEGER NOT NULL, customer_id TEXT NOT NULL, customer TEXT NOT NULL, city TEXT NOT NULL, market TEXT NOT NULL, platform TEXT NOT NULL, channel TEXT NOT NULL, product_id TEXT NOT NULL, product_name TEXT NOT NULL, category TEXT NOT NULL, sold_quantity INTEGER NOT NULL, price_cents INTEGER NOT NULL, revenue_cents INTEGER NOT NULL);
        CREATE VIEW analytics_monthly_sales AS SELECT substr(order_date, 1, 7) month, SUM(sold_quantity) quantity, SUM(revenue_cents) revenue_cents, COUNT(DISTINCT customer_id) customers FROM gold_sales GROUP BY month;
        CREATE VIEW analytics_category_sales AS SELECT category, SUM(sold_quantity) quantity, SUM(revenue_cents) revenue_cents FROM gold_sales GROUP BY category;
        CREATE VIEW analytics_product_sales AS SELECT product_name, category, SUM(sold_quantity) quantity, SUM(revenue_cents) revenue_cents FROM gold_sales GROUP BY product_id;
        CREATE VIEW analytics_customer_sales AS SELECT customer, city, channel, SUM(sold_quantity) quantity, SUM(revenue_cents) revenue_cents FROM gold_sales GROUP BY customer_id;
        CREATE VIEW analytics_channel_sales AS SELECT channel, SUM(sold_quantity) quantity, SUM(revenue_cents) revenue_cents FROM gold_sales GROUP BY channel;
    """)


def reject(db: sqlite3.Connection, source: str, record_id: str, reason: str) -> None:
    db.execute("INSERT INTO rejected_rows VALUES (?, ?, ?)", (source, record_id, reason))


def build(input_dir: Path = DEFAULT_DATA, db_path: Path = DEFAULT_DB) -> dict:
    input_dir, db_path = Path(input_dir), Path(db_path)
    customers = read_csv(input_dir / "customers.csv", {"customer_id", "customer_name", "city"})
    products = read_csv(input_dir / "products.csv", {"product_id", "product_name", "category"})
    prices = read_csv(input_dir / "gross_price.csv", {"product_id", "year", "price_inr"})
    order_files = sorted(input_dir.glob("orders*.csv"))
    if not order_files:
        raise FileNotFoundError(f"No orders*.csv files in {input_dir}")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    handle, temp_name = tempfile.mkstemp(prefix="fmcg-", suffix=".sqlite", dir=db_path.parent)
    os.close(handle)
    temp = Path(temp_name)
    try:
        with closing(sqlite3.connect(temp)) as db, db:
            schema(db)
            db.executemany("INSERT INTO bronze_customers VALUES (:customer_id,:customer_name,:city,:market,:platform,:channel)", [
                {**row, "market": row.get("market", ""), "platform": row.get("platform", ""), "channel": row.get("channel", "")} for row in customers
            ])
            db.executemany("INSERT INTO bronze_products VALUES (:product_id,:product_name,:category)", products)
            db.executemany("INSERT INTO bronze_prices VALUES (:product_id,:year,:price_inr)", prices)
            for row in customers:
                key = row["customer_id"].strip()
                name = " ".join(row["customer_name"].split()).title()
                city_raw = " ".join(row["city"].split())
                city = CITY_FIXES.get(city_raw.lower(), city_raw.title())
                if not key or not name or not city:
                    reject(db, "customers.csv", key, "missing required customer field")
                    continue
                if db.execute("SELECT 1 FROM silver_customers WHERE customer_id=?", (key,)).fetchone():
                    reject(db, "customers.csv", key, "duplicate customer_id")
                    continue
                db.execute("INSERT INTO silver_customers VALUES (?,?,?,?,?,?)", (key, name, city, row.get("market", "").strip() or "India", row.get("platform", "").strip() or "FMCG", row.get("channel", "").strip() or "Unknown"))
            for row in products:
                key = row["product_id"].strip()
                name, category = row["product_name"].strip(), row["category"].strip()
                if not key or not name or not category:
                    reject(db, "products.csv", key, "missing required product field")
                    continue
                if db.execute("SELECT 1 FROM silver_products WHERE product_id=?", (key,)).fetchone():
                    reject(db, "products.csv", key, "duplicate product_id")
                    continue
                db.execute("INSERT INTO silver_products VALUES (?,?,?)", (key, name, category))
            for row in prices:
                key = row["product_id"].strip()
                try:
                    year, price = int(row["year"]), cents(row["price_inr"])
                    if year < 2000 or year > 2100:
                        raise ValueError("invalid year")
                    if not db.execute("SELECT 1 FROM silver_products WHERE product_id=?", (key,)).fetchone():
                        raise ValueError("unknown product_id")
                    db.execute("INSERT INTO silver_prices VALUES (?,?,?)", (key, year, price))
                except (ValueError, sqlite3.IntegrityError) as error:
                    reject(db, "gross_price.csv", key, str(error))
            seen_orders: set[str] = set()
            for path in order_files:
                rows = read_csv(path, {"order_id", "customer_id", "product_id", "order_qty", "order_placement_date"})
                accepted = 0
                for row in rows:
                    order_id = row["order_id"].strip()
                    db.execute("INSERT INTO bronze_orders VALUES (?,?,?,?,?,?)", (path.name, order_id, row["customer_id"], row["product_id"], row["order_qty"], row["order_placement_date"]))
                    try:
                        if not order_id or order_id in seen_orders:
                            raise ValueError("missing or duplicate order_id")
                        seen_orders.add(order_id)
                        customer_id, product_id = row["customer_id"].strip(), row["product_id"].strip()
                        quantity, date = int(row["order_qty"]), iso_date(row["order_placement_date"])
                        if quantity <= 0:
                            raise ValueError("order quantity must be positive")
                        customer = db.execute("SELECT customer_name,city,market,platform,channel FROM silver_customers WHERE customer_id=?", (customer_id,)).fetchone()
                        product = db.execute("SELECT product_name,category FROM silver_products WHERE product_id=?", (product_id,)).fetchone()
                        if customer is None or product is None:
                            raise ValueError("unmapped customer_id or product_id")
                        year = int(date[:4])
                        price_row = db.execute("SELECT price_cents FROM silver_prices WHERE product_id=? AND year=?", (product_id, year)).fetchone()
                        if price_row is None:
                            raise ValueError("missing price for product and year")
                        db.execute("INSERT INTO silver_orders VALUES (?,?,?,?,?)", (order_id, customer_id, product_id, quantity, date))
                        db.execute("INSERT INTO gold_sales VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (order_id, date, year, customer_id, f"{customer[0]} - {customer[1]}", customer[1], customer[2], customer[3], customer[4], product_id, product[0], product[1], quantity, price_row[0], quantity * price_row[0]))
                        accepted += 1
                    except ValueError as error:
                        reject(db, path.name, order_id, str(error))
                db.execute("INSERT INTO pipeline_batch_audit VALUES (?,?,?,?)", (path.name, len(rows), accepted, len(rows)-accepted))
            summary = get_summary(db)
            if summary["orders"] == 0:
                raise ValueError("No valid orders were produced; inspect source data")
        os.replace(temp, db_path)
        return summary
    finally:
        temp.unlink(missing_ok=True)


def get_summary(db: sqlite3.Connection) -> dict:
    orders, quantity, revenue, customers, products = db.execute("SELECT COUNT(*), COALESCE(SUM(sold_quantity),0), COALESCE(SUM(revenue_cents),0), COUNT(DISTINCT customer_id), COUNT(DISTINCT product_id) FROM gold_sales").fetchone()
    return {"orders": orders, "quantity": quantity, "revenue_inr": revenue / 100, "customers": customers, "products": products, "rejected": db.execute("SELECT COUNT(*) FROM rejected_rows").fetchone()[0]}


def dashboard_data(db_path: Path) -> dict:
    with closing(sqlite3.connect(db_path)) as db:
        db.row_factory = sqlite3.Row
        output = {"summary": get_summary(db)}
        for name in ("monthly", "category", "product", "customer", "channel"):
            output[name] = [dict(row) for row in db.execute(f"SELECT * FROM analytics_{name}_sales ORDER BY revenue_cents DESC")]
        output["rejections"] = [dict(row) for row in db.execute("SELECT * FROM rejected_rows ORDER BY source_file,record_id")]
        output["batches"] = [dict(row) for row in db.execute("SELECT * FROM pipeline_batch_audit ORDER BY source_file")]
        return output


def answer(question: str, db_path: Path = DEFAULT_DB) -> dict:
    data = dashboard_data(db_path)
    q = question.lower()
    if "month" in q or "trend" in q:
        return {"question": question, "results": data["monthly"]}
    if "categor" in q:
        return {"question": question, "results": data["category"]}
    if "product" in q:
        return {"question": question, "results": data["product"]}
    if "customer" in q:
        return {"question": question, "results": data["customer"]}
    if "channel" in q:
        return {"question": question, "results": data["channel"]}
    if "revenue" in q or "sales" in q or "quantity" in q:
        return {"question": question, "results": data["summary"]}
    return {"question": question, "error": "Try asking about revenue, quantity, monthly trends, categories, products, customers, or channels."}


def serve(db_path: Path, host: str, port: int) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"Build the data first: python fmcg_local.py build --db {db_path}")
    html = (ROOT / "web" / "index.html").read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path == "/":
                content, kind, status = html, "text/html; charset=utf-8", 200
            elif parsed.path == "/api/data":
                content, kind, status = json.dumps(dashboard_data(db_path)).encode(), "application/json", 200
            else:
                content, kind, status = b"Not found", "text/plain", 404
            self.send_response(status)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

    print(f"Dashboard: http://{host}:{port}")
    ThreadingHTTPServer((host, port), Handler).serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the FMCG analytics project locally")
    sub = parser.add_subparsers(dest="command", required=True)
    build_cmd = sub.add_parser("build", help="Validate source CSVs and build Bronze, Silver, and Gold tables")
    build_cmd.add_argument("--input", type=Path, default=DEFAULT_DATA)
    build_cmd.add_argument("--db", type=Path, default=DEFAULT_DB)
    serve_cmd = sub.add_parser("serve", help="Open the local dashboard")
    serve_cmd.add_argument("--db", type=Path, default=DEFAULT_DB)
    serve_cmd.add_argument("--host", default="127.0.0.1")
    serve_cmd.add_argument("--port", type=int, default=8765)
    ask_cmd = sub.add_parser("ask", help="Ask a supported analytics question")
    ask_cmd.add_argument("question")
    ask_cmd.add_argument("--db", type=Path, default=DEFAULT_DB)
    args = parser.parse_args()
    if args.command == "build":
        print(json.dumps(build(args.input, args.db), indent=2))
    elif args.command == "serve":
        serve(args.db, args.host, args.port)
    elif args.command == "ask":
        print(json.dumps(answer(args.question, args.db), indent=2))


if __name__ == "__main__":
    main()
