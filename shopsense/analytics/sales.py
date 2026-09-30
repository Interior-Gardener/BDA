"""Gold layer - sales analytics aggregated with Spark and served from MongoDB."""

from datetime import date, timedelta

from pyspark.sql import Window
from pyspark.sql import functions as F

from shopsense.datagen.catalog import CITIES, SALE_EVENTS
from shopsense.mongo import write_df, write_docs


def _rev(df):
    return df.filter("is_revenue")


def kpis(spark, s, ref):
    """Headline KPI document (single doc)."""
    orders, lines, sessions = s["orders"], s["lines"], s["sessions"]
    placed = orders.count()
    r = _rev(orders).agg(
        F.sum("net_amount").alias("revenue"), F.count("*").alias("orders"),
        F.countDistinct("customer_id").alias("customers"), F.sum("units").alias("units"),
        F.sum("profit").alias("profit"), F.sum("discount_amount").alias("discount"),
    ).first()
    status = {x["status"]: x["count"] for x in orders.groupBy("status").count().collect()}
    per_cust = _rev(orders).groupBy("customer_id").count()
    repeat = per_cust.filter("count > 1").count()
    rating = lines.agg(F.avg("rating")).first()[0]
    ss = sessions.agg(F.count("*").alias("n"), F.sum("has_purchase").alias("p"),
                      F.sum("has_cart").alias("c"), F.avg("duration_sec").alias("d"),
                      F.avg("is_bounce").alias("b")).first()
    carts_no_buy = sessions.filter("has_cart = 1 AND has_purchase = 0").count()

    def window_rev(d0, d1):
        return _rev(orders).filter((F.col("order_date") > d0) & (F.col("order_date") <= d1)) \
            .agg(F.sum("net_amount"), F.count("*"), F.countDistinct("customer_id")).first()

    cur = window_rev(ref - timedelta(days=30), ref)
    prev = window_rev(ref - timedelta(days=60), ref - timedelta(days=30))

    def growth(a, b):
        return round((a - b) / b * 100, 2) if b else None

    doc = {
        "_id": "global",
        "as_of": ref.isoformat(),
        "revenue": round(r["revenue"], 2),
        "orders": r["orders"],
        "orders_placed": placed,
        "customers": r["customers"],
        "total_customers": s["customers"].count(),
        "units": r["units"],
        "profit": round(r["profit"], 2),
        "margin_pct": round(r["profit"] / r["revenue"] * 100, 2),
        "discount_given": round(r["discount"], 2),
        "aov": round(r["revenue"] / r["orders"], 2),
        "repeat_customer_rate": round(repeat / r["customers"] * 100, 2),
        "cancel_rate": round(status.get("Cancelled", 0) / placed * 100, 2),
        "return_rate": round(status.get("Returned", 0) / placed * 100, 2),
        "avg_rating": round(rating, 2) if rating else None,
        "sessions": ss["n"],
        "conversion_rate": round(ss["p"] / ss["n"] * 100, 2),
        "cart_abandonment_rate": round(carts_no_buy / ss["c"] * 100, 2),
        "avg_session_sec": round(ss["d"], 1),
        "bounce_rate": round(ss["b"] * 100, 2),
        "last30": {"revenue": round(cur[0] or 0, 2), "orders": cur[1], "customers": cur[2]},
        "prev30": {"revenue": round(prev[0] or 0, 2), "orders": prev[1], "customers": prev[2]},
        "growth": {"revenue": growth(cur[0] or 0, prev[0] or 0),
                   "orders": growth(cur[1], prev[1]),
                   "customers": growth(cur[2], prev[2])},
        "status_breakdown": status,
    }
    write_docs("kpis", [doc])
    return 1


def daily_and_monthly(spark, s):
    orders, lines = s["orders"], s["lines"]
    daily = _rev(orders).groupBy("order_date").agg(
        F.round(F.sum("net_amount"), 2).alias("revenue"),
        F.count("*").alias("orders"),
        F.countDistinct("customer_id").alias("customers"),
        F.sum("units").alias("units"),
        F.round(F.sum("profit"), 2).alias("profit"),
    ).withColumn("aov", F.round(F.col("revenue") / F.col("orders"), 2)) \
        .withColumnRenamed("order_date", "date")
    n1 = write_df(daily, "sales_daily", indexes=["date"])

    # daily x category grain -> lets the API filter by date range + category
    dcat = _rev(lines).groupBy("order_date", "category").agg(
        F.round(F.sum("revenue"), 2).alias("revenue"),
        F.countDistinct("order_id").alias("orders"),
        F.sum("quantity").alias("units"),
        F.round(F.sum("profit"), 2).alias("profit"),
    ).withColumnRenamed("order_date", "date")
    n2 = write_df(dcat, "sales_daily_category", indexes=["date", "category"])

    # monthly with new vs returning customers and MoM growth (window functions)
    w_first = Window.partitionBy("customer_id")
    o = _rev(orders).withColumn("first_month", F.min("order_month").over(w_first))
    monthly = o.groupBy("order_month").agg(
        F.round(F.sum("net_amount"), 2).alias("revenue"),
        F.count("*").alias("orders"),
        F.countDistinct("customer_id").alias("customers"),
        F.countDistinct(F.when(F.col("order_month") == F.col("first_month"),
                               F.col("customer_id"))).alias("new_customers"),
        F.round(F.sum("profit"), 2).alias("profit"),
        F.round(F.sum("discount_amount"), 2).alias("discount"),
    ).withColumn("returning_customers", F.col("customers") - F.col("new_customers")) \
        .withColumn("aov", F.round(F.col("revenue") / F.col("orders"), 2))
    w = Window.orderBy("order_month")
    monthly = monthly.withColumn("prev", F.lag("revenue").over(w)) \
        .withColumn("mom_growth", F.round((F.col("revenue") - F.col("prev")) / F.col("prev") * 100, 2)) \
        .drop("prev").withColumnRenamed("order_month", "month")
    n3 = write_df(monthly.coalesce(1), "sales_monthly", indexes=["month"])
    return n1 + n2 + n3


def breakdowns(spark, s):
    orders, lines, products = s["orders"], s["lines"], s["products"]
    total = _rev(orders).agg(F.sum("net_amount")).first()[0]

    cat = lines.groupBy("category").agg(
        F.round(F.sum(F.when(F.col("is_revenue"), F.col("revenue"))), 2).alias("revenue"),
        F.countDistinct(F.when(F.col("is_revenue"), F.col("order_id"))).alias("orders"),
        F.sum(F.when(F.col("is_revenue"), F.col("quantity"))).alias("units"),
        F.round(F.sum(F.when(F.col("is_revenue"), F.col("profit"))), 2).alias("profit"),
        F.round(F.avg("discount_pct") * 100, 2).alias("avg_discount_pct"),
        F.round(F.avg(F.when(F.col("status") == "Returned", 1).otherwise(0)) * 100, 2).alias("return_rate"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.countDistinct("product_id").alias("products"),
    ).withColumn("margin_pct", F.round(F.col("profit") / F.col("revenue") * 100, 2)) \
        .withColumn("share_pct", F.round(F.col("revenue") / F.lit(total) * 100, 2))
    n = write_df(cat.coalesce(1), "sales_by_category", id_col="category")

    sub = _rev(lines).groupBy("category", "sub_category").agg(
        F.round(F.sum("revenue"), 2).alias("revenue"),
        F.sum("quantity").alias("units"),
        F.round(F.sum("profit"), 2).alias("profit"))
    n += write_df(sub.coalesce(1), "sales_by_subcategory")

    # geography (joined with a small broadcast reference table of coordinates)
    geo_ref = spark.createDataFrame([(c[0], c[4], c[5]) for c in CITIES], ["city", "lat", "lon"])
    city = _rev(orders).filter(F.col("city") != "Unknown") \
        .groupBy("city", "state", "region", "city_tier").agg(
        F.round(F.sum("net_amount"), 2).alias("revenue"),
        F.count("*").alias("orders"),
        F.countDistinct("customer_id").alias("customers"),
    ).withColumn("aov", F.round(F.col("revenue") / F.col("orders"), 2)) \
        .join(F.broadcast(geo_ref), "city", "left")
    n += write_df(city.coalesce(1), "sales_by_city")

    state = _rev(orders).groupBy("state", "region").agg(
        F.round(F.sum("net_amount"), 2).alias("revenue"),
        F.count("*").alias("orders"),
        F.countDistinct("customer_id").alias("customers"))
    n += write_df(state.coalesce(1), "sales_by_state")

    # payment / device / acquisition channel dimensions -> one collection
    dims = []
    for dim in ("payment_method", "device", "city_tier", "region"):
        rows = orders.groupBy(dim).agg(
            F.round(F.sum(F.when(F.col("is_revenue"), F.col("net_amount"))), 2).alias("revenue"),
            F.count("*").alias("orders"),
            F.round(F.avg(F.when(F.col("status") == "Cancelled", 1).otherwise(0)) * 100, 2).alias("cancel_rate"),
            F.round(F.avg(F.when(F.col("status") == "Returned", 1).otherwise(0)) * 100, 2).alias("return_rate"),
            F.round(F.avg("net_amount"), 2).alias("aov"),
        ).collect()
        dims += [{"dimension": dim, "value": str(r[dim]), **{k: r[k] for k in r.asDict() if k != dim}}
                 for r in rows]
    chan = _rev(orders).join(s["customers"].select("customer_id", "acquisition_channel"), "customer_id") \
        .groupBy("acquisition_channel").agg(
            F.round(F.sum("net_amount"), 2).alias("revenue"), F.count("*").alias("orders"),
            F.countDistinct("customer_id").alias("customers"), F.round(F.avg("net_amount"), 2).alias("aov"))
    dims += [{"dimension": "acquisition_channel", "value": r["acquisition_channel"],
              **{k: r[k] for k in ("revenue", "orders", "customers", "aov")}} for r in chan.collect()]
    n += write_docs("sales_by_dimension", dims, indexes=["dimension"])

    # day-of-week x hour heat-map
    heat = _rev(orders).groupBy(
        F.dayofweek("order_ts").alias("dow"), F.hour("order_ts").alias("hour")
    ).agg(F.count("*").alias("orders"), F.round(F.sum("net_amount"), 2).alias("revenue"))
    n += write_df(heat.coalesce(1), "sales_heatmap")
    return n


def campaigns(spark, s):
    """Measure uplift of each named sale event against the non-sale baseline."""
    daily = _rev(s["orders"]).groupBy("order_date").agg(
        F.sum("net_amount").alias("revenue"), F.count("*").alias("orders")).collect()
    by_day = {r["order_date"]: (r["revenue"], r["orders"]) for r in daily}
    if not by_day:
        return 0
    days = sorted(by_day)
    tagged = {}
    for d in days:
        for name, mo, day, length, *_ in SALE_EVENTS:
            st = date(d.year, mo, day)
            if st <= d < st + timedelta(days=length):
                tagged[d] = name
    docs = {}
    for d, name in tagged.items():
        key = f"{name} {d.year}"
        doc = docs.setdefault(key, {"_id": key, "event": name, "year": d.year, "start": d.isoformat(),
                                    "days": 0, "revenue": 0.0, "orders": 0})
        doc["days"] += 1
        doc["revenue"] += by_day[d][0]
        doc["orders"] += by_day[d][1]
        doc["start"] = min(doc["start"], d.isoformat())
    for doc in docs.values():
        # baseline = average of the non-sale days in the 28 days before the event
        # (a local baseline removes the effect of long-term business growth)
        st = date.fromisoformat(doc["start"])
        window = [by_day[st - timedelta(days=i)][0] for i in range(1, 29)
                  if (st - timedelta(days=i)) in by_day and (st - timedelta(days=i)) not in tagged]
        base_avg = sum(window) / len(window) if window else None
        doc["revenue"] = round(doc["revenue"], 2)
        doc["avg_daily_revenue"] = round(doc["revenue"] / doc["days"], 2)
        doc["uplift_pct"] = round((doc["avg_daily_revenue"] / base_avg - 1) * 100, 2) if base_avg else None
        doc["baseline_daily_revenue"] = round(base_avg, 2) if base_avg else None
    return write_docs("campaigns", sorted((d for d in docs.values() if d["uplift_pct"] is not None),
                                         key=lambda x: x["start"]))


def product_stats(spark, s):
    """Per-product KPIs merged with the catalogue and clickstream conversion."""
    lines, events, products = s["lines"], s["events"], s["products"]
    sales = lines.groupBy("product_id").agg(
        F.round(F.sum(F.when(F.col("is_revenue"), F.col("revenue"))), 2).alias("revenue"),
        F.sum(F.when(F.col("is_revenue"), F.col("quantity"))).alias("units"),
        F.countDistinct("order_id").alias("orders"),
        F.countDistinct("customer_id").alias("buyers"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("rating").alias("ratings"),
        F.round(F.avg(F.when(F.col("status") == "Returned", 1).otherwise(0)) * 100, 2).alias("return_rate"),
        F.round(F.sum(F.when(F.col("is_revenue"), F.col("profit"))), 2).alias("profit"),
    )
    ev = events.filter(F.col("product_id").isNotNull()).groupBy("product_id").agg(
        F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("views"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("carts"),
        F.sum(F.when(F.col("event_type") == "add_to_wishlist", 1).otherwise(0)).alias("wishlists"),
    )
    w = Window.partitionBy("category").orderBy(F.desc("revenue"))
    df = products.join(sales, "product_id", "left").join(ev, "product_id", "left") \
        .fillna(0, subset=["revenue", "units", "orders", "buyers", "views", "carts", "wishlists", "profit"]) \
        .withColumn("view_to_buy_pct",
                    F.round(F.when(F.col("views") > 0, F.col("orders") / F.col("views") * 100), 2)) \
        .withColumn("cart_to_buy_pct",
                    F.round(F.when(F.col("carts") > 0, F.col("orders") / F.col("carts") * 100), 2)) \
        .withColumn("category_rank", F.rank().over(w))
    return write_df(df, "products", id_col="product_id",
                    indexes=["category", [("revenue", True)], "product_name"])
