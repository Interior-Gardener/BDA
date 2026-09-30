"""Churn prediction - supervised classification with a *time-based* split.

    |<------- observation window ------->| cutoff |<-- 90-day label window -->| ref
    features are computed only from data before the cutoff; the label is 1
    (churned) when the customer places no successful order in the next 90 days.

Three Spark MLlib models are trained and compared (Logistic Regression with
cross-validated regularisation, Random Forest, Gradient-Boosted Trees). The
best model (by ROC-AUC) scores every customer as of today. The logistic model
is exported to MongoDB so the web app can explain predictions and run a
live "what-if" simulator without starting Spark.
"""

import time
from datetime import timedelta

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import Window
from pyspark.sql import functions as F

from shopsense import config
from shopsense.mongo import bulk_update, get_db, write_docs

NUMERIC = ["recency_days", "frequency", "monetary", "aov", "tenure_days", "orders_90d",
           "orders_prev_90d", "spend_90d", "sessions_30d", "sessions_90d",
           "days_since_last_session", "cart_abandon_rate", "avg_discount_pct", "return_rate",
           "cancel_rate", "category_diversity", "cod_share", "age", "city_tier"]
LOG = ["recency_days", "frequency", "monetary", "aov", "spend_90d", "days_since_last_session"]
CATEGORICAL = ["preferred_device", "acquisition_channel"]

LABELS = {
    "recency_days": "Days since last order", "frequency": "Number of orders",
    "monetary": "Total spend", "aov": "Average order value", "tenure_days": "Customer tenure",
    "orders_90d": "Orders in last 90 days", "orders_prev_90d": "Orders in previous 90 days",
    "spend_90d": "Spend in last 90 days", "sessions_30d": "Sessions in last 30 days",
    "sessions_90d": "Sessions in last 90 days", "days_since_last_session": "Days since last visit",
    "cart_abandon_rate": "Cart abandonment rate", "avg_discount_pct": "Average discount %",
    "return_rate": "Return rate", "cancel_rate": "Cancellation rate",
    "category_diversity": "Categories purchased", "cod_share": "Cash-on-delivery share",
    "age": "Age", "city_tier": "City tier",
}


def build_features(s, as_of):
    """Behavioural features computed strictly from data *before* `as_of`."""
    o = s["orders"].filter(F.col("order_date") < F.lit(as_of))
    rev = o.filter("is_revenue")
    d90 = as_of - timedelta(days=90)
    d180 = as_of - timedelta(days=180)
    base = rev.groupBy("customer_id").agg(
        F.datediff(F.lit(as_of), F.max("order_date")).alias("recency_days"),
        F.count("*").alias("frequency"),
        F.sum("net_amount").alias("monetary"),
        F.avg("net_amount").alias("aov"),
        F.datediff(F.lit(as_of), F.min("order_date")).alias("tenure_days"),
        F.sum(F.when(F.col("order_date") >= F.lit(d90), 1).otherwise(0)).alias("orders_90d"),
        F.sum(F.when((F.col("order_date") >= F.lit(d180)) & (F.col("order_date") < F.lit(d90)), 1)
              .otherwise(0)).alias("orders_prev_90d"),
        F.sum(F.when(F.col("order_date") >= F.lit(d90), F.col("net_amount")).otherwise(0)).alias("spend_90d"),
        (F.avg(F.col("discount_amount") / F.col("gross_value")) * 100).alias("avg_discount_pct"),
    )
    placed = o.groupBy("customer_id").agg(
        F.avg(F.when(F.col("status") == "Returned", 1).otherwise(0)).alias("return_rate"),
        F.avg(F.when(F.col("status") == "Cancelled", 1).otherwise(0)).alias("cancel_rate"),
        F.avg(F.when(F.col("payment_method") == "Cash on Delivery", 1).otherwise(0)).alias("cod_share"),
    )
    dev = o.groupBy("customer_id", "device").count() \
        .withColumn("r", F.row_number().over(
            Window.partitionBy("customer_id").orderBy(F.desc("count")))).filter("r = 1") \
        .select("customer_id", F.col("device").alias("preferred_device"))
    div = s["lines"].filter(F.col("order_date") < F.lit(as_of)).filter("is_revenue") \
        .groupBy("customer_id").agg(F.countDistinct("category").alias("category_diversity"))
    ss = s["sessions"].filter(F.col("session_date") < F.lit(as_of))
    sess = ss.groupBy("customer_id").agg(
        F.sum(F.when(F.col("session_date") >= F.lit(as_of - timedelta(days=30)), 1).otherwise(0)).alias("sessions_30d"),
        F.sum(F.when(F.col("session_date") >= F.lit(d90), 1).otherwise(0)).alias("sessions_90d"),
        F.datediff(F.lit(as_of), F.max("session_date")).alias("days_since_last_session"),
        F.sum(F.when(F.col("session_date") >= F.lit(d180), F.col("has_cart")).otherwise(0)).alias("carts"),
        F.sum(F.when((F.col("session_date") >= F.lit(d180)) & (F.col("has_cart") == 1),
                     F.col("has_purchase")).otherwise(0)).alias("cart_buys"),
    ).withColumn("cart_abandon_rate",
                 F.when(F.col("carts") > 0, 1 - F.col("cart_buys") / F.col("carts")).otherwise(0.0)) \
        .drop("carts", "cart_buys")
    cust = s["customers"].select("customer_id", "age", "city_tier", "acquisition_channel")
    df = (base.join(placed, "customer_id", "left").join(dev, "customer_id", "left")
          .join(div, "customer_id", "left").join(sess, "customer_id", "left")
          .join(cust, "customer_id", "left")
          .fillna({"sessions_30d": 0, "sessions_90d": 0, "days_since_last_session": 365,
                   "cart_abandon_rate": 0.0, "category_diversity": 1, "return_rate": 0.0,
                   "cancel_rate": 0.0, "cod_share": 0.0, "preferred_device": "Mobile App",
                   "acquisition_channel": "Direct", "age": 30, "city_tier": 2}))
    for c in LOG:
        df = df.withColumn(f"log_{c}", F.log1p(F.col(c).cast("double")))
    return df


def _model_inputs():
    return [f"log_{c}" if c in LOG else c for c in NUMERIC] + [f"{c}_ohe" for c in CATEGORICAL]


def _prep_stages():
    idx = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in CATEGORICAL]
    ohe = OneHotEncoder(inputCols=[f"{c}_idx" for c in CATEGORICAL],
                        outputCols=[f"{c}_ohe" for c in CATEGORICAL], handleInvalid="keep")
    asm = VectorAssembler(inputCols=_model_inputs(), outputCol="raw_features")
    return idx + [ohe, asm]


def _metrics(pred, name):
    auc = BinaryClassificationEvaluator(metricName="areaUnderROC").evaluate(pred)
    pr = BinaryClassificationEvaluator(metricName="areaUnderPR").evaluate(pred)
    cm = {(int(r["label"]), int(r["prediction"])): r["count"]
          for r in pred.groupBy("label", "prediction").count().collect()}
    tp, fp = cm.get((1, 1), 0), cm.get((0, 1), 0)
    fn, tn = cm.get((1, 0), 0), cm.get((0, 0), 0)
    prec = tp / (tp + fp) if tp + fp else 0
    rec = tp / (tp + fn) if tp + fn else 0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0
    # ROC curve points (test set is small enough to collect)
    pl = pred.select(vector_to_array("probability")[1].alias("p"), "label").collect()
    p = np.array([r["p"] for r in pl])
    y = np.array([r["label"] for r in pl])
    roc = []
    for t in np.linspace(1, 0, 41):
        yhat = p >= t
        tpr = (yhat & (y == 1)).sum() / max((y == 1).sum(), 1)
        fpr = (yhat & (y == 0)).sum() / max((y == 0).sum(), 1)
        roc.append([round(float(fpr), 4), round(float(tpr), 4)])
    return {"model": name, "auc": round(auc, 4), "auc_pr": round(pr, 4),
            "accuracy": round((tp + tn) / max(tp + tn + fp + fn, 1), 4),
            "precision": round(prec, 4), "recall": round(rec, 4), "f1": round(f1, 4),
            "confusion": {"tp": tp, "fp": fp, "fn": fn, "tn": tn}, "roc": roc}


def _feature_names(model, df):
    attrs = model.transform(df.limit(5)).schema["raw_features"].metadata["ml_attr"]["attrs"]
    names = sorted([a for group in attrs.values() for a in group], key=lambda a: a["idx"])
    return [a["name"] for a in names]


def run(spark, s, ref):
    t0 = time.time()
    window = config.CHURN_WINDOW_DAYS
    cutoff = ref - timedelta(days=window - 1)

    # ------------------------------------------------ training data (as of cutoff)
    feats = build_features(s, cutoff)
    future = s["orders"].filter("is_revenue").filter(F.col("order_date") >= F.lit(cutoff)) \
        .select("customer_id").distinct().withColumn("bought", F.lit(1))
    data = feats.join(future, "customer_id", "left") \
        .withColumn("label", F.when(F.col("bought").isNull(), 1.0).otherwise(0.0)).drop("bought")
    data = data.cache()
    n = data.count()
    churn_rate = data.agg(F.avg("label")).first()[0]
    train, test = data.randomSplit([0.8, 0.2], seed=42)
    train.cache()
    test.cache()

    results, fitted = [], {}

    # Logistic regression with 3-fold cross-validated regularisation
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=100)
    lr_pipe = Pipeline(stages=_prep_stages() + [
        StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True), lr])
    grid = ParamGridBuilder().addGrid(lr.regParam, [0.001, 0.01, 0.1]).build()
    cv = CrossValidator(estimator=lr_pipe, estimatorParamMaps=grid, numFolds=3, seed=42,
                        evaluator=BinaryClassificationEvaluator(metricName="areaUnderROC"))
    cvm = cv.fit(train)
    fitted["Logistic Regression"] = cvm.bestModel
    best_lr = cvm.bestModel.stages[-1]
    lr_params = {"regParam": best_lr.getRegParam(), "elasticNetParam": best_lr.getElasticNetParam()}

    rf = Pipeline(stages=_prep_stages() + [RandomForestClassifier(
        featuresCol="raw_features", labelCol="label", numTrees=150, maxDepth=8, seed=42,
        featureSubsetStrategy="sqrt")])
    fitted["Random Forest"] = rf.fit(train)

    gbt = Pipeline(stages=_prep_stages() + [GBTClassifier(
        featuresCol="raw_features", labelCol="label", maxIter=50, maxDepth=5, stepSize=0.1, seed=42)])
    fitted["Gradient Boosted Trees"] = gbt.fit(train)

    for name, m in fitted.items():
        results.append(_metrics(m.transform(test), name))
    best = max(results, key=lambda r: r["auc"])
    best_model = fitted[best["model"]]

    names = _feature_names(fitted["Random Forest"], train)
    pretty = [LABELS.get(n.replace("log_", ""), n.replace("_ohe_", ": ").replace("_", " ").title())
              for n in names]
    tree_name = best["model"] if best["model"] != "Logistic Regression" else "Gradient Boosted Trees"
    imp = fitted[tree_name].stages[-1].featureImportances.toArray()
    importance = sorted([{"feature": pretty[i], "importance": round(float(v), 4)}
                         for i, v in enumerate(imp)], key=lambda d: -d["importance"])[:15]

    # ------------------------------------------------ export logistic model for the web app
    lr_model = fitted["Logistic Regression"]
    indexers = [st for st in lr_model.stages if st.__class__.__name__ == "StringIndexerModel"]
    scaler = [st for st in lr_model.stages if st.__class__.__name__ == "StandardScalerModel"][0]
    coef = best_lr.coefficients.toArray()
    lr_names = _feature_names(lr_model, train)
    lr_export = {
        "_id": "churn_logistic_export", "numeric": NUMERIC, "log_features": LOG,
        "categorical": {ix.getInputCol(): list(ix.labels) for ix in indexers},
        "feature_names": lr_names,
        "mean": [round(float(x), 6) for x in scaler.mean.toArray()],
        "std": [round(float(x), 6) for x in scaler.std.toArray()],
        "coef": [round(float(x), 6) for x in coef], "intercept": float(best_lr.intercept),
        "labels": LABELS,
    }
    get_db()["model_registry"].replace_one({"_id": lr_export["_id"]}, lr_export, upsert=True)

    # ------------------------------------------------ score everyone as of today
    now = build_features(s, ref + timedelta(days=1))
    scored = best_model.transform(now).withColumn(
        "churn_probability", F.round(vector_to_array("probability")[1], 4))
    rows = scored.select("customer_id", "churn_probability", *NUMERIC, *CATEGORICAL).collect()
    ups, dist = [], {"High": 0, "Medium": 0, "Low": 0}
    hist = [0] * 10
    rev_at_risk = 0.0
    for r in rows:
        p = r["churn_probability"]
        band = "High" if p >= 0.7 else ("Medium" if p >= 0.4 else "Low")
        dist[band] += 1
        hist[min(int(p * 10), 9)] += 1
        tenure_m = max(r["tenure_days"], 30) / 30.0
        monthly = r["monetary"] / tenure_m
        clv = monthly * 12 * (1 - p)
        if band == "High":
            rev_at_risk += monthly * 12
        ups.append((r["customer_id"], {
            "churn_probability": p, "churn_risk": band, "clv_12m": round(clv, 2),
            "churn_features": {c: (round(r[c], 4) if isinstance(r[c], float) else r[c])
                               for c in NUMERIC + CATEGORICAL},
        }))
    bulk_update("customers", ups)

    for r in results:
        get_db()["model_registry"].replace_one({"_id": "churn_" + r["model"].lower().replace(" ", "_")}, {
            "_id": "churn_" + r["model"].lower().replace(" ", "_"), "task": "Churn Prediction",
            "algorithm": r["model"] + " (Spark MLlib)",
            "metrics": {k: r[k] for k in ("auc", "auc_pr", "accuracy", "precision", "recall", "f1")},
            "is_best": r["model"] == best["model"], "train_rows": train.count(), "test_rows": test.count(),
            "trained_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            **({"best_params": lr_params} if r["model"] == "Logistic Regression" else {}),
        }, upsert=True)

    write_docs("churn_summary", [{
        "_id": "summary", "cutoff": cutoff.isoformat(), "as_of": ref.isoformat(),
        "label_window_days": window, "training_customers": n,
        "training_churn_rate": round(churn_rate * 100, 2),
        "models": results, "best_model": best["model"], "feature_importance": importance,
        "importance_model": tree_name,
        "risk_distribution": dist, "probability_histogram": hist,
        "revenue_at_risk_12m": round(rev_at_risk, 2), "scored_customers": len(rows),
        "lr_best_params": lr_params, "train_seconds": round(time.time() - t0, 2),
    }])
    data.unpersist()
    train.unpersist()
    test.unpersist()
    return len(rows)
