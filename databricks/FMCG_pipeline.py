# Databricks notebook source
"""Reproducible FMCG Bronze → Silver → Gold pipeline.

Run this notebook on Databricks Runtime in a schema where you can create tables.
Leave path widgets empty for bundled demonstration data, or supply four CSV URIs.
Each run replaces derived tables, so rerunning a batch cannot double count orders.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("target_schema", "default", "Writable schema in current catalog")
for name in ("customers_path", "products_path", "gross_price_path", "orders_path"):
    dbutils.widgets.text(name, "", name)

schema_name = dbutils.widgets.get("target_schema").strip()
if not schema_name.replace("_", "").isalnum() or not schema_name[0].isalpha():
    raise ValueError("target_schema must be a simple schema name")
spark.sql(f"USE SCHEMA `{schema_name}`")
spark.conf.set("spark.sql.ansi.enabled", "false")


def table(name):
    return f"fmcg_{name}"


def save(df, name):
    (df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(table(name)))
    return spark.table(table(name))


def source(widget, rows, columns):
    path = dbutils.widgets.get(widget).strip()
    if path:
        result = spark.read.option("header", "true").csv(path)
    else:
        result = spark.createDataFrame(rows, columns)
    missing = set(columns) - set(result.columns)
    if missing:
        raise ValueError(f"{widget} is missing columns: {sorted(missing)}")
    return result


customers_raw = source("customers_path", [
    ("C001", "anita stores", "Bengaluruu", "India", "Retail", "General Trade"),
    ("C002", "Ravi Mart", "Hyderabad", "India", "Retail", "Modern Trade"),
    ("C003", "Fresh Basket", "NewDelhi", "India", "Online", "E-commerce"),
    ("C004", "Urban Grocers", "Bengaluru", "India", "Retail", "Modern Trade"),
], ["customer_id", "customer_name", "city", "market", "platform", "channel"])
products_raw = source("products_path", [
    ("P001", "Orange Juice 1L", "Beverages"),
    ("P002", "Wholegrain Biscuits", "Snacks"),
    ("P003", "Herbal Shampoo", "Personal Care"),
    ("P004", "Green Tea 250g", "Beverages"),
], ["product_id", "product_name", "category"])
prices_raw = source("gross_price_path", [
    ("P001", "2025", "120.00"), ("P002", "2025", "60.00"),
    ("P003", "2025", "180.00"), ("P004", "2025", "150.00"),
    ("P001", "2026", "130.00"), ("P002", "2026", "65.00"),
    ("P003", "2026", "190.00"), ("P004", "2026", "160.00"),
], ["product_id", "year", "price_inr"])
orders_raw = source("orders_path", [
    ("O001", "C001", "P001", "10", "2025-09-02"),
    ("O002", "C002", "P002", "15", "2025/09/04"),
    ("O003", "C003", "P003", "4", "05-09-2025"),
    ("O004", "C004", "P004", "8", "2025-10-03"),
    ("O005", "C001", "P002", "12", "2025-10-07"),
    ("O006", "C003", "P001", "6", "2026-01-08"),
    ("O007", "C002", "P003", "3", "2026-02-02"),
    ("O008", "C004", "P004", "5", "2026-02-18"),
    ("O009", "C004", "P002", "0", "2026-02-19"),
], ["order_id", "customer_id", "product_id", "order_qty", "order_placement_date"])

customers_bronze = save(customers_raw, "bronze_customers")
products_bronze = save(products_raw, "bronze_products")
prices_bronze = save(prices_raw, "bronze_gross_price")
orders_bronze = save(orders_raw, "bronze_orders")

city = F.lower(F.trim(F.col("city")))
customers = (customers_bronze
    .withColumn("customer_id", F.trim("customer_id"))
    .withColumn("customer_name", F.initcap(F.trim("customer_name")))
    .withColumn("city", F.when(city.isin("bengaluruu", "bengalore"), "Bengaluru")
        .when(city.isin("hyderabadd", "hyderbad"), "Hyderabad")
        .when(city.isin("newdelhi", "newdheli", "newdelhee"), "New Delhi")
        .otherwise(F.initcap(F.trim("city"))))
    .withColumn("market", F.coalesce(F.col("market"), F.lit("India")))
    .withColumn("platform", F.coalesce(F.col("platform"), F.lit("FMCG")))
    .withColumn("channel", F.coalesce(F.col("channel"), F.lit("Unknown")))
    .filter((F.col("customer_id") != "") & (F.col("customer_name") != "") & (F.col("city") != ""))
    .dropDuplicates(["customer_id"]))
customers = save(customers, "silver_customers")

products = (products_bronze
    .select(F.trim("product_id").alias("product_id"),
            F.trim("product_name").alias("product_name"),
            F.trim("category").alias("category"))
    .filter((F.col("product_id") != "") & (F.col("product_name") != "") & (F.col("category") != ""))
    .dropDuplicates(["product_id"]))
products = save(products, "silver_products")

prices = (prices_bronze
    .select(F.trim("product_id").alias("product_id"), F.col("year").cast("int").alias("year"),
            F.col("price_inr").cast("decimal(18,2)").alias("price_inr"))
    .filter((F.col("year").between(2000, 2100)) & (F.col("price_inr") > 0))
    .dropDuplicates(["product_id", "year"]))
prices = save(prices, "silver_gross_price")

orders = (orders_bronze
    .select(F.trim("order_id").alias("order_id"),
            F.trim("customer_id").alias("customer_id"),
            F.trim("product_id").alias("product_id"),
            F.col("order_qty").cast("long").alias("order_qty"),
            F.coalesce(F.to_date("order_placement_date", "yyyy-MM-dd"),
                       F.to_date("order_placement_date", "yyyy/MM/dd"),
                       F.to_date("order_placement_date", "dd-MM-yyyy"),
                       F.to_date("order_placement_date", "dd/MM/yyyy"),
                       F.to_date("order_placement_date", "EEEE, MMMM d, yyyy")).alias("order_date"))
    .filter((F.col("order_id") != "") & (F.col("order_qty") > 0) & F.col("order_date").isNotNull())
    .withColumn("rn", F.row_number().over(Window.partitionBy("order_id").orderBy("order_date")))
    .filter(F.col("rn") == 1).drop("rn"))
orders = save(orders, "silver_orders")

sales = (orders.alias("o")
    .join(customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "inner")
    .join(products.alias("p"), F.col("o.product_id") == F.col("p.product_id"), "inner")
    .join(prices.alias("g"), (F.col("o.product_id") == F.col("g.product_id")) &
          (F.year("o.order_date") == F.col("g.year")), "inner")
    .select(F.col("o.order_id"), F.col("o.order_date").alias("date"),
            F.year("o.order_date").alias("year"), F.col("c.customer_id"),
            F.concat_ws(" - ", F.col("c.customer_name"), F.col("c.city")).alias("customer"),
            F.col("c.city"), F.col("c.market"), F.col("c.platform"), F.col("c.channel"),
            F.col("p.product_id"), F.col("p.product_name"), F.col("p.category"),
            F.col("o.order_qty").alias("sold_quantity"), F.col("g.price_inr"))
    .withColumn("revenue_inr", (F.col("sold_quantity") * F.col("price_inr")).cast("decimal(20,2)")))
sales = save(sales, "gold_sales")

save(sales.groupBy(F.date_format("date", "yyyy-MM").alias("month"))
    .agg(F.sum("sold_quantity").alias("quantity"), F.sum("revenue_inr").alias("revenue_inr"),
         F.countDistinct("customer_id").alias("customers")), "gold_monthly_sales")
save(sales.groupBy("category").agg(F.sum("sold_quantity").alias("quantity"),
    F.sum("revenue_inr").alias("revenue_inr")), "gold_category_sales")
save(sales.groupBy("product_id", "product_name", "category")
    .agg(F.sum("sold_quantity").alias("quantity"), F.sum("revenue_inr").alias("revenue_inr")), "gold_product_sales")
save(sales.groupBy("customer_id", "customer", "city", "channel")
    .agg(F.sum("sold_quantity").alias("quantity"), F.sum("revenue_inr").alias("revenue_inr")), "gold_customer_sales")
save(sales.groupBy("channel").agg(F.sum("sold_quantity").alias("quantity"),
    F.sum("revenue_inr").alias("revenue_inr")), "gold_channel_sales")

source_count, valid_count, gold_count = orders_bronze.count(), orders.count(), sales.count()
revenue = sales.agg(F.sum("revenue_inr")).first()[0]
if gold_count == 0:
    raise ValueError("No sales reached Gold; check IDs, prices, dates, and schema")
print(f"Bronze orders={source_count}; Silver valid orders={valid_count}; Gold sales={gold_count}")
print(f"Rejected before Silver={source_count-valid_count}; unmatched after Silver={valid_count-gold_count}")
print(f"Revenue INR={revenue}; target schema={schema_name}")
display(spark.table(table("gold_monthly_sales")).orderBy("month"))
