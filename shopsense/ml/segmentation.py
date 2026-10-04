"""Customer segmentation - RFM feature engineering + K-Means clustering
(Spark MLlib) with silhouette-based model selection, automatic business
naming of clusters, classic RFM 1-5 scores and a 2-D PCA projection."""

import time

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import PCA, StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Window
from pyspark.sql import functions as F

from shopsense.mongo import bulk_update, get_db, write_docs

RAW_FEATURES = ["recency_days", "frequency", "monetary", "aov", "tenure_days",
                "category_diversity", "avg_discount_pct"]
LOG_FEATURES = ["recency_days", "frequency", "monetary", "aov"]

PLAYBOOK = {
    "Champions": "Reward them: early access to sales, loyalty tier upgrades, referral programme.",
    "Loyal Customers": "Up-sell premium products and bundles; invite to membership programme.",
    "Potential Loyalists": "Personalised recommendations and a 2nd-purchase coupon to build habit.",
    "New & Promising": "Onboarding journey, welcome offers and category discovery e-mails.",
    "Needs Attention": "Time-limited offers on favourite categories to re-ignite interest.",
    "At Risk": "Win-back campaign: personalised discount + 'we miss you' push notification.",
    "About to Sleep": "Share bestsellers and limited-time deals before they lapse completely.",
    "Can't Lose Them": "High-value but lapsed: personal outreach, VIP win-back offer, feedback survey.",
    "Hibernating": "Low-cost reactivation (e-mail/app push) during big sale events only.",
    "Lost": "Exclude from paid campaigns; include in seasonal mass mailers.",
    "Prospects": "Browsed but never bought: retarget with first-order discount and free shipping.",
}
ALL_NAMES = ["Champions", "Loyal Customers", "Potential Loyalists", "New & Promising", "Needs Attention",
             "At Risk", "About to Sleep", "Can't Lose Them", "Hibernating", "Lost"]


def rfm_features(s, ref):
    rev = s["orders"].filter("is_revenue")
    f = rev.groupBy("customer_id").agg(
        F.datediff(F.lit(ref), F.max("order_date")).alias("recency_days"),
        F.count("*").alias("frequency"),
        F.round(F.sum("net_amount"), 2).alias("monetary"),
        F.round(F.avg("net_amount"), 2).alias("aov"),
        F.datediff(F.lit(ref), F.min("order_date")).alias("tenure_days"),
        F.round(F.avg(F.col("discount_amount") / F.col("gross_value")) * 100, 2).alias("avg_discount_pct"),
    )
    div = s["lines"].filter("is_revenue").groupBy("customer_id") \
        .agg(F.countDistinct("category").alias("category_diversity"))
    return f.join(div, "customer_id", "left").fillna(0)


def run(spark, s, ref):
    t0 = time.time()
    df = rfm_features(s, ref)
    for c in LOG_FEATURES:
        df = df.withColumn(f"log_{c}", F.log1p(F.col(c)))
    feats = [f"log_{c}" if c in LOG_FEATURES else c for c in RAW_FEATURES]
    prep = Pipeline(stages=[
        VectorAssembler(inputCols=feats, outputCol="raw_vec"),
        StandardScaler(inputCol="raw_vec", outputCol="features", withMean=True, withStd=True),
    ]).fit(df)
    data = prep.transform(df).cache()
    data.count()

    # ---- model selection: silhouette for k = 3..8
    evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
    curve, models = [], {}
    for k in range(3, 9):
        m = KMeans(k=k, seed=42, featuresCol="features", maxIter=40).fit(data)
        sil = evaluator.evaluate(m.transform(data))
        curve.append({"k": k, "silhouette": round(sil, 4), "wssse": round(m.summary.trainingCost, 2)})
        models[k] = m
    # pick the best silhouette among business-interpretable k (4..6)
    best = max((c for c in curve if 4 <= c["k"] <= 6), key=lambda c: c["silhouette"])
    k = best["k"]
    model = models[k]
    pred = model.transform(data).cache()

    # ---- name clusters from their centroids
    stats = pred.groupBy("prediction").agg(
        F.count("*").alias("customers"),
        *[F.avg(c).alias(c) for c in RAW_FEATURES],
        *[F.avg(f"log_{c}").alias(f"log_{c}") for c in ("frequency", "monetary")],
        F.sum("monetary").alias("revenue"),
    ).collect()
    glob = pred.agg(*[F.avg(c).alias(f"m_{c}") for c in ("log_frequency", "log_monetary")],
                    *[F.stddev(c).alias(f"s_{c}") for c in ("log_frequency", "log_monetary")]).first()

    def z(v, c):
        return (v - glob[f"m_{c}"]) / (glob[f"s_{c}"] or 1)

    info = []
    for r in stats:
        info.append({"cluster": r["prediction"], "row": r,
                     "value": z(r["log_frequency"], "log_frequency") + z(r["log_monetary"], "log_monetary")})
    # business naming rules on the centroid (recency in days, frequency, tenure)
    used = set()

    def take(*cands):
        for c in cands + tuple(ALL_NAMES):
            if c not in used:
                used.add(c)
                return c

    for c in sorted(info, key=lambda i: -i["value"]):
        r = c["row"]
        rec, freq, ten = r["recency_days"], r["frequency"], r["tenure_days"]
        if rec <= 90:
            if ten < 150 and freq < 2.5:
                c["name"] = take("New & Promising", "Potential Loyalists")
            else:
                c["name"] = take("Champions", "Loyal Customers", "Potential Loyalists")
        elif rec <= 240:
            c["name"] = take("At Risk", "Needs Attention") if freq >= 2.5 else take("Needs Attention", "About to Sleep")
        else:
            c["name"] = take("Can't Lose Them", "Hibernating") if freq >= 4 else take("Hibernating", "Lost")
    names = {c["cluster"]: c["name"] for c in info}

    total_rev = sum(r["revenue"] for r in stats)
    total_cust = sum(r["customers"] for r in stats)
    seg_docs = []
    for c in info:
        r = c["row"]
        seg_docs.append({
            "_id": c["name"], "segment": c["name"], "cluster": c["cluster"],
            "customers": r["customers"], "share_pct": round(r["customers"] / total_cust * 100, 2),
            "revenue": round(r["revenue"], 2), "revenue_share_pct": round(r["revenue"] / total_rev * 100, 2),
            **{f"avg_{f}": round(r[f], 2) for f in RAW_FEATURES},
            "action": PLAYBOOK[c["name"]],
        })
    prospects = get_db()["customers"].count_documents({"orders": 0})
    seg_docs.append({"_id": "Prospects", "segment": "Prospects", "cluster": -1, "customers": prospects,
                     "share_pct": None, "revenue": 0, "revenue_share_pct": 0, "action": PLAYBOOK["Prospects"]})
    seg_docs.sort(key=lambda d: -d["revenue"])
    write_docs("segments", seg_docs)

    # ---- classic RFM quintile scores (1-5) using window functions
    name_map = F.create_map(*[x for kv in names.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
    scored = pred.withColumn("segment", name_map[F.col("prediction")]) \
        .withColumn("R", 6 - F.ntile(5).over(Window.orderBy("recency_days"))) \
        .withColumn("F", F.ntile(5).over(Window.orderBy("frequency"))) \
        .withColumn("M", F.ntile(5).over(Window.orderBy("monetary")))

    # ---- 2-D PCA projection for the scatter plot
    pca = PCA(k=2, inputCol="features", outputCol="pc").fit(pred)
    pts = pca.transform(scored).select("customer_id", "segment", vector_to_array("pc").alias("pc"),
                                       "monetary", "frequency", "recency_days")
    sample = pts.sample(fraction=min(1.0, 2500 / max(total_cust, 1)), seed=7).collect()
    write_docs("segment_points", [{"customer_id": r["customer_id"], "segment": r["segment"],
                                   "x": round(r["pc"][0], 4), "y": round(r["pc"][1], 4),
                                   "monetary": r["monetary"], "frequency": r["frequency"],
                                   "recency_days": r["recency_days"]} for r in sample])

    rows = scored.select("customer_id", "segment", "prediction", "R", "F", "M",
                         "tenure_days", "category_diversity", "avg_discount_pct").collect()
    bulk_update("customers", [(r["customer_id"], {
        "segment": r["segment"], "cluster": r["prediction"],
        "rfm": {"R": r["R"], "F": r["F"], "M": r["M"], "score": f"{r['R']}{r['F']}{r['M']}"},
        "tenure_days": r["tenure_days"], "category_diversity": r["category_diversity"],
        "avg_discount_pct": r["avg_discount_pct"],
    }) for r in rows])

    centers = np.array(model.clusterCenters()).round(4).tolist()
    get_db()["model_registry"].replace_one({"_id": "segmentation_kmeans"}, {
        "_id": "segmentation_kmeans", "task": "Customer Segmentation", "algorithm": "K-Means (Spark MLlib)",
        "features": feats, "k": k, "silhouette": best["silhouette"], "silhouette_curve": curve,
        "cluster_names": {str(k_): v for k_, v in names.items()}, "centers_scaled": centers,
        "pca_explained_variance": [round(float(x), 4) for x in pca.explainedVariance],
        "trained_at": time.strftime("%Y-%m-%d %H:%M:%S"), "train_rows": total_cust,
        "train_seconds": round(time.time() - t0, 2),
    }, upsert=True)
    data.unpersist()
    pred.unpersist()
    return total_cust
