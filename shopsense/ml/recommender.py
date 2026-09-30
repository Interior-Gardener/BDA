"""Personalised product recommendations - implicit-feedback collaborative
filtering with Spark MLlib ALS (Alternating Least Squares).

Implicit signal strength per (customer, product):
    5 x purchases + 2 x add-to-cart + 1.5 x wishlist + 0.5 x product views

Offline evaluation uses *leave-last-purchase-out*: the most recent product
each customer bought is hidden, the model is trained on everything else and
we check whether the hidden product appears in the Top-10 (HitRate@10,
NDCG@10). A popularity recommender is evaluated the same way as a baseline.
"""

import math
import time

import numpy as np
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.sql import Window
from pyspark.sql import functions as F

from shopsense.mongo import bulk_update, get_db, write_df

K = 10


def interactions(s):
    buys = s["lines"].filter(F.col("status") != "Cancelled").groupBy("customer_id", "product_id") \
        .agg(F.count("*").alias("buys"))
    ev = s["events"].filter(F.col("product_id").isNotNull()).groupBy("customer_id", "product_id").agg(
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("carts"),
        F.sum(F.when(F.col("event_type") == "add_to_wishlist", 1).otherwise(0)).alias("wish"),
        F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("views"))
    df = ev.join(buys, ["customer_id", "product_id"], "full").fillna(0)
    return df.withColumn("strength", F.least(F.lit(30.0),
                         5 * F.col("buys") + 2 * F.col("carts") + 1.5 * F.col("wish") + 0.5 * F.col("views")))


def _rank_metrics(ranked, holdout):
    """ranked: customer_id, product_id, rank ; holdout: customer_id, product_id."""
    hits = ranked.join(holdout, ["customer_id", "product_id"]).select("customer_id", "rank").collect()
    n = holdout.count()
    hr = len(hits) / n if n else 0
    ndcg = sum(1 / math.log2(r["rank"] + 1) for r in hits) / n if n else 0
    return round(hr, 4), round(ndcg, 4)


def _top_k_excluding(recs, seen, k):
    w = Window.partitionBy("customer_id").orderBy(F.desc("score"))
    return recs.join(seen, ["customer_id", "product_id"], "left_anti") \
        .withColumn("rank", F.row_number().over(w)).filter(F.col("rank") <= k)


def _als(rank, reg, alpha=15.0):
    return ALS(userCol="uid", itemCol="iid", ratingCol="strength", implicitPrefs=True, rank=rank,
               regParam=reg, alpha=alpha, maxIter=12, coldStartStrategy="drop", nonnegative=True, seed=42)


def _recommend(model, users, n):
    recs = model.recommendForUserSubset(users, n)
    return recs.select("uid", F.explode("recommendations").alias("r")) \
        .select("uid", F.col("r.iid").alias("iid"), F.col("r.rating").alias("score"))


def run(spark, s, ref):
    t0 = time.time()
    inter = interactions(s)
    uidx = StringIndexer(inputCol="customer_id", outputCol="uid").fit(inter)
    iidx = StringIndexer(inputCol="product_id", outputCol="iid").fit(inter)
    data = iidx.transform(uidx.transform(inter)) \
        .withColumn("uid", F.col("uid").cast("int")).withColumn("iid", F.col("iid").cast("int")).cache()
    data.count()
    ids = data.select("customer_id", "uid", "product_id", "iid")
    umap = ids.select("customer_id", "uid").distinct()
    imap = ids.select("product_id", "iid").distinct()

    # ------------------------------------------------ leave-last-purchase-out evaluation
    purch = s["lines"].filter(F.col("status") != "Cancelled").select("customer_id", "product_id", "order_ts")
    w = Window.partitionBy("customer_id").orderBy(F.desc("order_ts"), F.desc("product_id"))
    last = purch.withColumn("rn", F.row_number().over(w))
    eligible = purch.groupBy("customer_id").agg(F.countDistinct("product_id").alias("np")).filter("np >= 3")
    holdout = last.filter("rn = 1").join(eligible, "customer_id").select("customer_id", "product_id")
    # the held-out item must not appear earlier for that customer
    earlier = last.filter("rn > 1").select("customer_id", "product_id").distinct()
    holdout = holdout.join(earlier, ["customer_id", "product_id"], "left_anti").cache()
    train = data.join(holdout, ["customer_id", "product_id"], "left_anti").cache()
    seen_train = train.filter("buys > 0").select("customer_id", "product_id")
    test_users = holdout.join(umap, "customer_id").select("uid").distinct()

    grid = []
    for rank, reg, alpha in [(16, 0.05, 15.0), (32, 1.0, 5.0), (64, 1.0, 5.0), (64, 1.0, 10.0)]:
        m = _als(rank, reg, alpha).fit(train)
        recs = _recommend(m, test_users, K + 30).join(umap, "uid").join(imap, "iid")
        top = _top_k_excluding(recs.select("customer_id", "product_id", "score"), seen_train, K)
        hr, ndcg = _rank_metrics(top, holdout)
        grid.append({"rank": rank, "regParam": reg, "alpha": alpha, "hit_rate": hr, "ndcg": ndcg})
    best = max(grid, key=lambda g: g["hit_rate"])

    pop = train.groupBy("product_id").agg(F.sum("buys").alias("score"))
    pop_recs = holdout.select("customer_id").crossJoin(
        pop.orderBy(F.desc("score")).limit(K + 30))
    pop_top = _top_k_excluding(pop_recs, seen_train, K)
    pop_hr, pop_ndcg = _rank_metrics(pop_top, holdout)

    # ------------------------------------------------ final model on all data
    model = _als(best["rank"], best["regParam"], best["alpha"]).fit(data)
    seen = data.filter("buys > 0").select("customer_id", "product_id")
    users = umap.select("uid")
    recs = _recommend(model, users, K + 30).join(umap, "uid").join(imap, "iid") \
        .select("customer_id", "product_id", "score")
    top = _top_k_excluding(recs, seen, K)
    cats = s["lines"].filter("is_revenue").select("customer_id", "category").distinct() \
        .withColumn("known_cat", F.lit(True))
    prod = s["products"].select("product_id", "product_name", "category", "sub_category", "brand", "list_price")
    top = top.join(prod, "product_id").join(cats, ["customer_id", "category"], "left") \
        .withColumn("reason", F.when(F.col("known_cat"), F.concat(F.lit("Because you shop "), F.col("category")))
                    .otherwise(F.lit("Customers like you also bought this")))
    grouped = top.groupBy("customer_id").agg(F.sort_array(F.collect_list(F.struct(
        "rank", "product_id", "product_name", "category", "sub_category", "brand", "list_price",
        F.round("score", 4).alias("score"), "reason"))).alias("items"))
    n = write_df(grouped, "recommendations", id_col="customer_id")

    popular = s["lines"].filter("is_revenue").groupBy("product_id").agg(F.count("*").alias("n")) \
        .orderBy(F.desc("n")).limit(K).join(prod, "product_id").orderBy(F.desc("n")).collect()
    get_db()["recommendations"].replace_one({"_id": "__popular__"}, {
        "_id": "__popular__", "items": [{"rank": i + 1, **{k: r[k] for k in prod.columns},
                                         "score": None, "reason": "Trending best-seller"}
                                        for i, r in enumerate(popular)]}, upsert=True)

    # ------------------------------------------------ item-item similarity from latent factors
    fac = model.itemFactors.join(imap.withColumnRenamed("iid", "id"), "id").collect()
    pids = [r["product_id"] for r in fac]
    mat = np.array([r["features"] for r in fac], dtype=float)
    mat /= np.linalg.norm(mat, axis=1, keepdims=True) + 1e-9
    sim = mat @ mat.T
    np.fill_diagonal(sim, -1)
    ups = []
    for i, pid in enumerate(pids):
        best_j = np.argsort(-sim[i])[:8]
        ups.append((pid, {"similar": [{"product_id": pids[j], "score": round(float(sim[i, j]), 4)}
                                      for j in best_j]}))
    bulk_update("products", ups)

    get_db()["model_registry"].replace_one({"_id": "recommender_als"}, {
        "_id": "recommender_als", "task": "Product Recommendation",
        "algorithm": "ALS implicit collaborative filtering (Spark MLlib)",
        "params": {"rank": best["rank"], "regParam": best["regParam"], "alpha": best["alpha"], "maxIter": 12},
        "metrics": {"hit_rate@10": best["hit_rate"], "ndcg@10": best["ndcg"],
                    "popularity_hit_rate@10": pop_hr, "popularity_ndcg@10": pop_ndcg,
                    "lift_vs_popularity": round(best["hit_rate"] / pop_hr, 2) if pop_hr else None},
        "grid": grid, "interactions": data.count(), "users": umap.count(), "items": imap.count(),
        "eval_users": holdout.count(), "trained_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "train_seconds": round(time.time() - t0, 2),
    }, upsert=True)
    for df in (data, holdout, train):
        df.unpersist()
    return n
