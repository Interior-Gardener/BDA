"""Bronze layer - read the raw landing-zone files with explicit schemas."""

from pyspark.sql import types as T

from shopsense import config

CUSTOMER_SCHEMA = T.StructType([
    T.StructField("customer_id", T.StringType()),
    T.StructField("first_name", T.StringType()),
    T.StructField("last_name", T.StringType()),
    T.StructField("email", T.StringType()),
    T.StructField("gender", T.StringType()),
    T.StructField("age", T.IntegerType()),
    T.StructField("city", T.StringType()),
    T.StructField("state", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("city_tier", T.IntegerType()),
    T.StructField("signup_date", T.DateType()),
    T.StructField("acquisition_channel", T.StringType()),
])

PRODUCT_SCHEMA = T.StructType([
    T.StructField("product_id", T.StringType()),
    T.StructField("product_name", T.StringType()),
    T.StructField("category", T.StringType()),
    T.StructField("sub_category", T.StringType()),
    T.StructField("brand", T.StringType()),
    T.StructField("list_price", T.DoubleType()),
    T.StructField("cost_price", T.DoubleType()),
    T.StructField("launch_date", T.DateType()),
])

ORDER_SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("order_ts", T.TimestampType()),
    T.StructField("status", T.StringType()),
    T.StructField("payment_method", T.StringType()),
    T.StructField("device", T.StringType()),
    T.StructField("coupon_code", T.StringType()),
    T.StructField("shipping_fee", T.DoubleType()),
])

ITEM_SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("line_no", T.IntegerType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("list_price", T.DoubleType()),
    T.StructField("unit_price", T.DoubleType()),
    T.StructField("discount_pct", T.DoubleType()),
    T.StructField("rating", T.IntegerType()),
])

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("session_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("device", T.StringType()),
    T.StructField("traffic_source", T.StringType()),
    T.StructField("order_id", T.StringType()),
])


def _csv(spark, name, schema):
    return (spark.read.option("header", True).option("mode", "PERMISSIVE")
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .schema(schema).csv(str(config.RAW_DIR / name)))


def read_raw(spark):
    """Return a dict of bronze DataFrames."""
    raw = config.RAW_DIR
    if not (raw / "orders.csv").exists():
        raise FileNotFoundError(
            f"No raw data found in {raw}. Run `python run_pipeline.py --generate` first.")
    return {
        "customers": _csv(spark, "customers.csv", CUSTOMER_SCHEMA),
        "products": spark.read.schema(PRODUCT_SCHEMA).json(str(raw / "products.jsonl")),
        "orders": _csv(spark, "orders.csv", ORDER_SCHEMA),
        "order_items": _csv(spark, "order_items.csv", ITEM_SCHEMA),
        # a whole folder of monthly JSON-lines log files is read in parallel
        "events": (spark.read.schema(EVENT_SCHEMA)
                   .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                   .json(str(raw / "events"))),
    }
