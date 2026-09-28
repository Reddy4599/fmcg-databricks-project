# Run the FMCG project

This repository contains a runnable local pipeline and a Databricks notebook. The older notebooks in `fcmg_Code/`, `setup/`, and `FMCG_AI_Assistant/` record the original exploratory work and depend on the original S3 bucket and pre-existing workspace tables. Start with the commands below.

## Local run (Windows, Python 3.11+)

No pip packages, Java, cloud credentials, or paid account are needed.

```powershell
cd C:\Users\ASUS\Music\fmcg-databricks-project
python fmcg_local.py build
python fmcg_local.py serve
```

Open <http://127.0.0.1:8765/>. The included **fictional sample data** yields 8 accepted orders, 63 units, and ₹6,890 revenue. One zero-quantity order is rejected and listed in the dashboard.

Other useful commands:

```powershell
python fmcg_local.py ask "revenue by category"
python -m unittest -v test_fmcg_local
```

The local SQLite database is generated at `local/fmcg.sqlite` and is excluded from Git. Each `build` creates a new database and replaces the old one only after validation succeeds. This makes reruns safe and prevents incremental orders from being counted twice.

### Use your own data

Put these files in a folder and run `python fmcg_local.py build --input C:\path\to\folder`:

| File | Required columns |
| --- | --- |
| `customers.csv` | `customer_id,customer_name,city` (optional: `market,platform,channel`) |
| `products.csv` | `product_id,product_name,category` |
| `gross_price.csv` | `product_id,year,price_inr` |
| `orders.csv` | `order_id,customer_id,product_id,order_qty,order_placement_date` |

Additional files named `orders*.csv` are included automatically. Give each real order a stable, unique `order_id`; duplicate IDs are rejected. An order reaches Gold only when its customer, product, year-specific price, date, and positive quantity are valid. Supported dates: `YYYY-MM-DD`, `YYYY/MM/DD`, `DD-MM-YYYY`, `DD/MM/YYYY`, and `Monday, Month D, YYYY`. Prices have at most two decimal places.

Bronze tables preserve source values, Silver tables hold standardized records, and `gold_sales` has one row per accepted order. Analytics views summarize monthly, category, product, customer, and channel performance. `rejected_rows` and `pipeline_batch_audit` show why rows were excluded.

## Databricks run

`databricks/FMCG_pipeline.py` is a self-contained Databricks notebook source file. Import it into a workspace or open this repository as a Databricks Git folder, then run it on a Databricks Runtime in a schema where you can create Delta tables.

1. In a fresh workspace, run `CREATE SCHEMA IF NOT EXISTS workspace.fmcg_demo` in the SQL editor. Set `target_catalog` and `target_schema` to a location where you can create tables; their defaults are `workspace` and `fmcg_demo`.
2. Leave the four path widgets empty for the same fictional sample data as the local demo. To use real data, set **all four** widgets to accessible CSV paths. The CSVs must have the columns listed above, including `market,platform,channel` for customers.
3. Run all cells. The notebook writes `fmcg_bronze_*`, `fmcg_silver_*`, `fmcg_gold_sales`, and five `fmcg_gold_*_sales` Delta tables in the selected schema. It prints row counts and revenue and displays monthly sales.

The notebook was verified on Databricks serverless compute with the fictional sample: 8 Gold orders, 63 units, and ₹6,890 revenue across 14 Delta tables. It rebuilds derived tables on each run, so a rerun cannot add a batch twice. It uses Databricks-managed storage and does not require a personal AWS S3 account for the sample run. The original hard-coded S3 bucket remains a separate private data source; real-data runs require accessible CSV files or appropriate bucket access. This repository does not include or request credentials.

## Scope of the assistant and dashboard

The local `ask` command recognizes a small set of keywords; it is a rule-based query helper, not an LLM. The live local dashboard is served by `fmcg_local.py`. The original dashboard screenshots and PDF are retained as historical previews; there is no editable `.pbix` file in this repository. Databricks Genie must be configured separately in a Databricks workspace if desired.
