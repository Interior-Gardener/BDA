"""Gold layer - customer behaviour: conversion funnel, sessions, cohorts,
and the operational `orders` collection (document model with embedded items)."""

from pyspark.sql import Window
from pyspark.sql import functions as F

from shopsense.mongo import write_df, write_docs

FUNNEL_STEPS = [("Sessions", None), ("Viewed Product", "has_view"), ("Added to Cart", "has_cart"),
                ("Checkout", "has_checkout"), ("Purchased", "has_purchase")]


def _funnel_rows(df, dim=None):
    aggs = [F.count("*").alias("Sessions")] + [F.sum(c).alias(n) for n, c in FUNNEL_STEPS[1:]]
    aggs += [F.round(F.avg("duration_sec"), 1).alias("avg_duration_sec"),
             F.round(F.avg("events"), 2).alias("avg_events"),
             F.round(F.avg("is_bounce") * 100, 2).alias("bounce_rate")]
    rows = (df.groupBy(dim).agg(*aggs) if dim else df.agg(*aggs)).collect()
    out = []
    for r in rows:
        steps = [{"step": n, "sessions": int(r[n])} for n, _ in FUNNEL_STEPS]
        top = steps[0]["sessions"] or 1
        for i, st in enumerate(steps):
            st["pct_of_sessions"] = round(st["sessions"] / top * 100, 2)
            prev = steps[i - 1]["sessions"] if i else st["sessions"]
            st["step_conversion"] = round(st["sessions"] / prev * 100, 2) if prev else 0
        carts = r["Added to Cart"] or 1
        out.append({
            "dimension": dim or "all",
            "value": str(r[dim]) if dim else "All",
            "steps": steps,
            "conversion_rate": round(r["Purchased"] / top * 100, 2),
            "cart_abandonment_rate": round((1 - r["Purchased"] / carts) * 100, 2),
            "avg_duration_sec": r["avg_duration_sec"],
            "avg_events": r["avg_events"],
            "bounce_rate": r["bounce_rate"],
        })
    return out


def funnel(spark, s):
    sessions = s["sessions"]
    docs = _funnel_rows(sessions)
    for dim in ("device", "traffic_source"):
        docs += _funnel_rows(sessions, dim)
    return write_docs("funnel", docs, indexes=["dimension"])


def traffic(spark, s):
    sessions, events = s["sessions"], s["events"]
    hourly = sessions.groupBy(F.hour("start_time").alias("hour")).agg(
        F.count("*").alias("sessions"), F.sum("has_purchase").alias("purchases"))
    n = write_df(hourly.coalesce(1), "traffic_hourly")
    monthly = sessions.groupBy(F.date_format("start_time", "yyyy-MM").alias("month")).agg(
        F.count("*").alias("sessions"),
        F.countDistinct("customer_id").alias("visitors"),
        F.sum("has_purchase").alias("purchases"),
        F.round(F.avg("duration_sec"), 1).alias("avg_duration_sec"),
    ).withColumn("conversion_rate", F.round(F.col("purchases") / F.col("sessions") * 100, 2))
    n += write_df(monthly.coalesce(1), "traffic_monthly", indexes=["month"])
    ev = events.groupBy("event_type").count().collect()
    n += write_docs("event_counts", [{"event_type": r["event_type"], "count": r["count"]} for r in ev])
    return n


def cohorts(spark, s):
    """Monthly acquisition cohorts (month of first purchase) x months since."""
    o = s["orders"].filter("is_revenue").select("customer_id", "order_date")
    first = o.groupBy("customer_id").agg(F.min("order_date").alias("first_date"))
    act = o.join(first, "customer_id").withColumn(
        "cohort", F.date_format("first_date", "yyyy-MM")).withColumn(
        "age", F.floor(F.months_between(F.trunc("order_date", "month"),
                                        F.trunc("first_date", "month"))).cast("int"))
    counts = act.groupBy("cohort", "age").agg(F.countDistinct("customer_id").alias("customers"))
    size = counts.filter("age = 0").select("cohort", F.col("customers").alias("size"))
    rows = counts.join(size, "cohort").orderBy("cohort", "age").collect()
    docs = {}
    for r in rows:
        d = docs.setdefault(r["cohort"], {"_id": r["cohort"], "cohort": r["cohort"],
                                          "size": r["size"], "retention": []})
        d["retention"].append({"month": r["age"], "customers": r["customers"],
                               "pct": round(r["customers"] / r["size"] * 100, 2)})
    return write_docs("cohorts", sorted(docs.values(), key=lambda d: d["cohort"]))


def orders_collection(spark, s):
    """Operational order documents with embedded line items (MongoDB document model)."""
    items = s["lines"].groupBy("order_id").agg(F.collect_list(F.struct(
        "line_no", "product_id", "product_name", "category", "quantity", "list_price",
        "unit_price", "discount_pct", "revenue", "rating")).alias("items"))
    df = s["orders"].join(items, "order_id").select(
        "order_id", "customer_id", "order_ts", "order_date", "status", "payment_method", "device",
        "coupon_code", "shipping_fee", "items_count", "units", "gross_value", "net_amount",
        "discount_amount", "profit", "city", "state", "items")
    return write_df(df, "orders", id_col="order_id",
                    indexes=["customer_id", [("order_ts", True)], "status"])


def customer_profiles(spark, s, ref):
    """Base Customer-360 profile (ML stages later enrich these documents)."""
    orders, lines, sessions, customers = s["orders"], s["lines"], s["sessions"], s["customers"]
    rev = orders.filter("is_revenue")
    agg = orders.groupBy("customer_id").agg(
        F.count("*").alias("orders_placed"),
        F.sum(F.when(F.col("status") == "Cancelled", 1).otherwise(0)).alias("cancelled"),
        F.sum(F.when(F.col("status") == "Returned", 1).otherwise(0)).alias("returned"),
        F.min("order_date").alias("first_order"),
        F.max("order_date").alias("last_order"),
    )
    money = rev.groupBy("customer_id").agg(
        F.count("*").alias("orders"),
        F.round(F.sum("net_amount"), 2).alias("total_spent"),
        F.round(F.avg("net_amount"), 2).alias("aov"),
        F.round(F.sum("discount_amount"), 2).alias("total_discount"),
    )
    w = Window.partitionBy("customer_id").orderBy(F.desc("spend"))
    fav = lines.filter("is_revenue").groupBy("customer_id", "category") \
        .agg(F.sum("revenue").alias("spend")) \
        .withColumn("rn", F.row_number().over(w)).filter("rn = 1") \
        .select("customer_id", F.col("category").alias("favourite_category"))
    wdev = Window.partitionBy("customer_id").orderBy(F.desc("n"))
    dev = orders.groupBy("customer_id", "device").agg(F.count("*").alias("n")) \
        .withColumn("rn", F.row_number().over(wdev)).filter("rn = 1") \
        .select("customer_id", F.col("device").alias("preferred_device"))
    pay = orders.groupBy("customer_id", "payment_method").agg(F.count("*").alias("n")) \
        .withColumn("rn", F.row_number().over(wdev)).filter("rn = 1") \
        .select("customer_id", F.col("payment_method").alias("preferred_payment"))
    sess = sessions.groupBy("customer_id").agg(
        F.count("*").alias("sessions"),
        F.max("session_date").alias("last_seen"),
        F.round(F.avg("duration_sec"), 1).alias("avg_session_sec"),
        F.sum("has_cart").alias("cart_sessions"),
        F.sum("has_purchase").alias("purchase_sessions"),
    )
    df = (customers.select("customer_id", "full_name", "email", "gender", "age", "city", "state",
                           "region", "city_tier", "signup_date", "acquisition_channel")
          .join(agg, "customer_id", "left").join(money, "customer_id", "left")
          .join(fav, "customer_id", "left").join(dev, "customer_id", "left")
          .join(pay, "customer_id", "left").join(sess, "customer_id", "left")
          .fillna(0, subset=["orders", "orders_placed", "total_spent", "sessions", "cancelled",
                             "returned", "cart_sessions", "purchase_sessions"])
          .withColumn("recency_days", F.datediff(F.lit(ref), F.col("last_order")))
          .withColumn("segment", F.when(F.col("orders") == 0, "Prospects").otherwise(None))
          .withColumn("search_name", F.lower("full_name")))
    return write_df(df, "customers", id_col="customer_id",
                    indexes=["segment", "search_name", "city", [("total_spent", True)]])
