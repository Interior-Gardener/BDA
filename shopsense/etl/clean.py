"""Silver layer - cleaning, validation, de-duplication and enrichment.

Every rule applied here is counted so that the dashboard can show a
*data-quality report* for each pipeline run.
"""

from pyspark import StorageLevel
from pyspark.sql import functions as F

from shopsense import config
from shopsense.datagen.catalog import CITIES, PAYMENT_METHODS

VALID_EVENTS = ["page_view", "product_view", "add_to_cart", "remove_from_cart",
                "add_to_wishlist", "checkout", "purchase"]


def _canonical(col, values):
    """Map messy strings (case / whitespace variants) onto canonical values."""
    expr = None
    key = F.lower(F.trim(col))
    for v in values:
        cond = key == v.lower()
        expr = F.when(cond, F.lit(v)) if expr is None else expr.when(cond, F.lit(v))
    return expr.otherwise(F.initcap(F.trim(col)))


def _blank_to_null(df, cols):
    for c in cols:
        df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c)))
    return df


def clean(spark, bronze):
    dq = {}

    # ------------------------------------------------------------ customers
    c = bronze["customers"]
    raw_n = c.count()
    city_names = [x[0] for x in CITIES]
    c = c.withColumn("city", F.when(F.trim(F.col("city")) == "", None)
                     .otherwise(_canonical(F.col("city"), city_names)))
    missing_city = c.filter(F.col("city").isNull()).count()
    c = c.fillna({"city": "Unknown"}).dropDuplicates(["customer_id"])
    c = c.withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
    customers = c.persist(StorageLevel.MEMORY_AND_DISK)
    n = customers.count()
    dq["customers"] = {"raw": raw_n, "clean": n, "duplicates_removed": raw_n - n,
                       "missing_city_filled": missing_city,
                       "city_names_standardised": True}

    # ------------------------------------------------------------ products
    p = bronze["products"]
    raw_n = p.count()
    products = p.dropDuplicates(["product_id"]).filter(F.col("list_price") > 0) \
        .withColumn("margin_pct", F.round(1 - F.col("cost_price") / F.col("list_price"), 4)) \
        .persist(StorageLevel.MEMORY_AND_DISK)
    n = products.count()
    dq["products"] = {"raw": raw_n, "clean": n, "invalid_removed": raw_n - n}

    # ------------------------------------------------------------ orders
    o = _blank_to_null(bronze["orders"], ["coupon_code"])
    raw_n = o.count()
    bad_payment = o.filter(~F.col("payment_method").isin(PAYMENT_METHODS)).count()
    o = o.withColumn("payment_method", _canonical(F.col("payment_method"), PAYMENT_METHODS)) \
        .withColumn("status", F.initcap(F.trim("status"))) \
        .dropDuplicates(["order_id"]) \
        .filter(F.col("order_ts").isNotNull() & F.col("customer_id").isNotNull())
    orders_clean = o
    n_orders = o.count()
    dq["orders"] = {"raw": raw_n, "clean": n_orders, "duplicates_removed": raw_n - n_orders,
                    "payment_values_standardised": bad_payment}

    # ------------------------------------------------------------ order items
    it = bronze["order_items"]
    raw_n = it.count()
    invalid_qty = it.filter((F.col("quantity") <= 0) | F.col("quantity").isNull()).count()
    it = it.filter((F.col("quantity") > 0) & (F.col("unit_price") > 0)) \
        .dropDuplicates(["order_id", "line_no"])
    n = it.count()
    dq["order_items"] = {"raw": raw_n, "clean": n, "invalid_quantity_removed": invalid_qty}

    # ------------------------------------------------------------ events
    e = _blank_to_null(bronze["events"], ["product_id", "order_id"])
    raw_n = e.count()
    upper = e.filter(F.col("event_type") != F.lower(F.col("event_type"))).count()
    e = e.withColumn("event_type", F.lower(F.trim("event_type"))) \
        .dropDuplicates(["event_id"])
    after_dup = e.count()
    e = e.filter(F.col("event_type").isin(VALID_EVENTS)) \
        .filter(~((F.col("event_type") == "product_view") & F.col("product_id").isNull()))
    events = e.persist(StorageLevel.MEMORY_AND_DISK)
    n = events.count()
    dq["events"] = {"raw": raw_n, "clean": n, "duplicates_removed": raw_n - after_dup,
                    "event_type_case_fixed": upper, "invalid_removed": after_dup - n}

    # ------------------------------------------------------------ enrichment
    lines = (it.join(orders_clean.select("order_id", "customer_id", "order_ts", "status",
                                         "payment_method", "device"), "order_id")
             .join(products.select("product_id", "product_name", "category", "sub_category",
                                   "brand", "cost_price"), "product_id")
             .withColumn("revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))
             .withColumn("gross_value", F.round(F.col("quantity") * F.col("list_price"), 2))
             .withColumn("cost", F.round(F.col("quantity") * F.col("cost_price"), 2))
             .withColumn("profit", F.round(F.col("revenue") - F.col("cost"), 2))
             .withColumn("discount_amount", F.round(F.col("gross_value") - F.col("revenue"), 2))
             .withColumn("order_date", F.to_date("order_ts"))
             .withColumn("is_revenue", ~F.col("status").isin("Cancelled", "Returned")))
    lines = lines.persist(StorageLevel.MEMORY_AND_DISK)

    totals = lines.groupBy("order_id").agg(
        F.count("*").alias("items_count"),
        F.sum("quantity").alias("units"),
        F.round(F.sum("gross_value"), 2).alias("gross_value"),
        F.round(F.sum("revenue"), 2).alias("net_amount"),
        F.round(F.sum("discount_amount"), 2).alias("discount_amount"),
        F.round(F.sum("profit"), 2).alias("profit"),
        F.collect_set("category").alias("categories"),
    )
    orders = (orders_clean.join(totals, "order_id")
              .join(customers.select("customer_id", "city", "state", "region", "city_tier"),
                    "customer_id", "left")
              .withColumn("order_date", F.to_date("order_ts"))
              .withColumn("order_month", F.date_format("order_ts", "yyyy-MM"))
              .withColumn("is_revenue", ~F.col("status").isin("Cancelled", "Returned"))
              .withColumn("coupon_used", F.col("coupon_code").isNotNull()))
    orders = orders.persist(StorageLevel.MEMORY_AND_DISK)
    orders.count()

    # sessions (sessionised clickstream)
    sessions = (events.groupBy("session_id").agg(
        F.first("customer_id").alias("customer_id"),
        F.min("event_time").alias("start_time"),
        F.max("event_time").alias("end_time"),
        F.count("*").alias("events"),
        F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("product_views"),
        F.max(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("has_cart"),
        F.max(F.when(F.col("event_type") == "checkout", 1).otherwise(0)).alias("has_checkout"),
        F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("has_purchase"),
        F.first("device").alias("device"),
        F.first("traffic_source").alias("traffic_source"),
    ).withColumn("duration_sec",
                 F.col("end_time").cast("long") - F.col("start_time").cast("long"))
     .withColumn("has_view", (F.col("product_views") > 0).cast("int"))
     .withColumn("is_bounce", (F.col("events") == 1).cast("int"))
     .withColumn("session_date", F.to_date("start_time")))
    sessions = sessions.persist(StorageLevel.MEMORY_AND_DISK)
    n_sessions = sessions.count()
    dq["sessions"] = {"raw": n_sessions, "clean": n_sessions}

    silver = {"customers": customers, "products": products, "orders": orders,
              "lines": lines, "events": events, "sessions": sessions}

    if config.save_parquet_enabled():
        for name in ("orders", "lines", "sessions"):
            silver[name].write.mode("overwrite").parquet(str(config.LAKE_DIR / name))
        dq["parquet_lake"] = str(config.LAKE_DIR)

    # latest timestamp in the data = "today" for all as-of calculations
    ref = orders.agg(F.max("order_date")).first()[0]
    return silver, dq, ref

