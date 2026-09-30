"""Sales forecasting + anomaly detection.

Hybrid model (per series: total revenue and each category):
    log(revenue_t) = trend(t) + seasonality(calendar_t) + e_t
    * trend        - Spark MLlib LinearRegression on the time index
    * seasonality  - Spark MLlib RandomForestRegressor on calendar features
                     (day-of-week, month, day-of-month, salary days, and the
                      *planned* marketing calendar of sale events)

The last 60 days are held out to compare against three baselines
(seasonal-naive, 28-day moving average, calendar linear regression).
The model is then re-fitted on the full history to forecast the next
60 days with 80% / 95% prediction intervals.

Anomaly detection re-uses the fitted model: days whose actual revenue
deviates from the model's *expected* order volume by more than 4 robust
standard deviations (median absolute deviation) are flagged. Because the
expectation already includes weekends and sale events, a Diwali spike is
*not* an anomaly - a payment-gateway outage is.
"""

import json
import math
import time
from datetime import date, timedelta

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.sql import functions as F

from shopsense import config
from shopsense.datagen.catalog import SALE_EVENTS
from shopsense.mongo import get_db, write_docs

CAL_FEATURES = ["dow", "month", "dom", "is_weekend", "is_salary", "sale_flag", "sale_day", "days_to_month_end"]
HOLDOUT = 60


def _calendar(d: date):
    sale_flag, sale_day = 0, -1
    for _, mo, day, length, *_ in SALE_EVENTS:
        st = date(d.year, mo, day)
        if st <= d < st + timedelta(days=length):
            sale_flag, sale_day = 1, (d - st).days
    nxt = date(d.year + (d.month == 12), d.month % 12 + 1, 1)
    return {"dow": d.weekday(), "month": d.month, "dom": d.day, "is_weekend": int(d.weekday() >= 5),
            "is_salary": int(d.day <= 5), "sale_flag": sale_flag, "sale_day": sale_day,
            "days_to_month_end": (nxt - d).days}


def _frame(spark, dates, t0, values=None):
    rows = []
    for i, d in enumerate(dates):
        r = {"date": d.isoformat(), "t": float((d - t0).days), **_calendar(d)}
        if values is not None:
            r["y"] = float(math.log1p(values[i]))
        rows.append(r)
    return spark.createDataFrame(rows)


class HybridForecaster:
    def fit(self, spark, dates, values):
        self.t0 = dates[0]
        df = _frame(spark, dates, self.t0, values)
        self.trend = Pipeline(stages=[VectorAssembler(inputCols=["t"], outputCol="tf"),
                                      LinearRegression(featuresCol="tf", labelCol="y",
                                                       predictionCol="trend")]).fit(df)
        res = self.trend.transform(df).withColumn("resid", F.col("y") - F.col("trend"))
        self.season = Pipeline(stages=[
            VectorAssembler(inputCols=CAL_FEATURES, outputCol="cf"),
            RandomForestRegressor(featuresCol="cf", labelCol="resid", predictionCol="season", numTrees=120,
                                  maxDepth=7, minInstancesPerNode=4, featureSubsetStrategy="0.6",
                                  seed=42)]).fit(res)
        self.smear = 1.0
        fitted = self.predict(spark, dates)
        resid = np.log1p(np.asarray(values)) - np.log1p(fitted)
        self.sigma = float(np.std(resid))
        # Duan's smearing estimator removes the bias of back-transforming log predictions
        self.smear = float(np.mean(np.exp(resid)))
        return fitted * self.smear

    def predict(self, spark, dates):
        df = _frame(spark, dates, self.t0)
        out = self.season.transform(self.trend.transform(df)).select("date", "trend", "season").collect()
        by = {r["date"]: math.expm1(r["trend"] + r["season"]) for r in out}
        return np.array([max(by[d.isoformat()], 0.0) for d in dates]) * self.smear


def _calendar_lr(spark, dates, values, test_dates):
    t0 = dates[0]
    df = _frame(spark, dates, t0, values)
    ohe = OneHotEncoder(inputCols=["dow", "month"], outputCols=["dow_v", "month_v"])
    pipe = Pipeline(stages=[ohe, VectorAssembler(inputCols=["t", "dow_v", "month_v", "is_salary", "sale_flag"],
                                                  outputCol="f"),
                            LinearRegression(featuresCol="f", labelCol="y")]).fit(df)
    out = pipe.transform(_frame(spark, test_dates, t0)).select("date", "prediction").collect()
    by = {r["date"]: math.expm1(r["prediction"]) for r in out}
    return np.array([by[d.isoformat()] for d in test_dates])


def _metrics(y, p):
    y, p = np.asarray(y, float), np.asarray(p, float)
    mask = y > 0
    mape = float(np.mean(np.abs((y[mask] - p[mask]) / y[mask])) * 100)
    rmse = float(np.sqrt(np.mean((y - p) ** 2)))
    mae = float(np.mean(np.abs(y - p)))
    r2 = float(1 - np.sum((y - p) ** 2) / np.sum((y - y.mean()) ** 2))
    n = len(y) // 7 * 7
    yw, pw = y[-n:].reshape(-1, 7).sum(1), p[-n:].reshape(-1, 7).sum(1)
    wmape = float(np.mean(np.abs(yw - pw) / yw) * 100)
    return {"mape": round(mape, 2), "weekly_mape": round(wmape, 2), "rmse": round(rmse, 2),
            "mae": round(mae, 2), "r2": round(r2, 4)}


def _series(rows, start, end, key="revenue"):
    by = {r["d"]: r[key] for r in rows}
    dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    return dates, np.array([float(by.get(d, 0.0) or 0.0) for d in dates])


def forecast_series(spark, name, dates, values):
    train_d, test_d = dates[:-HOLDOUT], dates[-HOLDOUT:]
    train_y, test_y = values[:-HOLDOUT], values[-HOLDOUT:]

    # ---- evaluation on the hold-out window
    hf = HybridForecaster()
    hf.fit(spark, train_d, train_y)
    pred = hf.predict(spark, test_d)
    last_week = train_y[-7:]
    snaive = np.array([last_week[(i) % 7] for i in range(HOLDOUT)])
    ma = np.full(HOLDOUT, train_y[-28:].mean())
    lr = _calendar_lr(spark, train_d, train_y, test_d)
    evaluation = [
        {"model": "Hybrid LR-trend + RF-seasonality", **_metrics(test_y, pred)},
        {"model": "Calendar Linear Regression", **_metrics(test_y, lr)},
        {"model": "Seasonal Naive (last week)", **_metrics(test_y, snaive)},
        {"model": "28-day Moving Average", **_metrics(test_y, ma)},
    ]

    # ---- refit on everything and forecast the future
    final = HybridForecaster()
    fitted = final.fit(spark, dates, values)
    fut_dates = [dates[-1] + timedelta(days=i + 1) for i in range(config.FORECAST_HORIZON_DAYS)]
    fut = final.predict(spark, fut_dates)
    sig = final.sigma
    forecast = [{"date": d.isoformat(), "yhat": round(v, 2),
                 "lo80": round(math.expm1(math.log1p(v) - 1.2816 * sig), 2),
                 "hi80": round(math.expm1(math.log1p(v) + 1.2816 * sig), 2),
                 "lo95": round(math.expm1(math.log1p(v) - 1.96 * sig), 2),
                 "hi95": round(math.expm1(math.log1p(v) + 1.96 * sig), 2)}
                for d, v in zip(fut_dates, fut)]
    history = [{"date": d.isoformat(), "actual": round(float(a), 2), "fitted": round(float(f), 2)}
               for d, a, f in zip(dates, values, fitted)]
    holdout = [{"date": d.isoformat(), "actual": round(float(a), 2), "predicted": round(float(p), 2)}
               for d, a, p in zip(test_d, test_y, pred)]
    doc = {"_id": name, "series": name, "history": history, "holdout": holdout, "forecast": forecast,
           "evaluation": evaluation, "sigma_log": round(sig, 4),
           "next_30_days": round(float(fut[:30].sum()), 2),
           "last_30_days": round(float(values[-30:].sum()), 2)}
    return doc, fitted


def detect_anomalies(spark, dates, revenue, rev_fitted, counts):
    """Flag days whose order volume deviates strongly from the model expectation.
    Order counts are far less noisy than revenue (no heavy-tailed basket values),
    so the calendar-aware hybrid model is fitted on the daily order series."""
    fitted = HybridForecaster().fit(spark, dates, counts)
    resid = np.log1p(counts) - np.log1p(fitted)
    med = np.median(resid)
    mad = np.median(np.abs(resid - med)) * 1.4826 or 1e-6
    z = (resid - med) / mad
    out = []
    for i, d in enumerate(dates):
        zz = z[i]
        if abs(zz) >= 4.0:
            out.append({"type": "daily_revenue", "date": d.isoformat(),
                        "actual": round(float(revenue[i]), 2), "expected": round(float(rev_fitted[i]), 2),
                        "orders": int(counts[i]), "expected_orders": round(float(fitted[i]), 1),
                        "z_score": round(float(zz), 2), "direction": "spike" if zz > 0 else "drop",
                        "deviation_pct": round((counts[i] / fitted[i] - 1) * 100, 2) if fitted[i] else None,
                        "severity": "critical" if abs(zz) >= 7 else "warning"})
    return out


def run(spark, s, ref):
    t0 = time.time()
    orders = s["orders"].filter("is_revenue")
    rows = orders.groupBy(F.col("order_date").alias("d")).agg(
        F.sum("net_amount").alias("revenue"), F.count("*").alias("orders")).collect()
    start = min(r["d"] for r in rows)
    dates, values = _series(rows, start, ref)
    _, counts = _series(rows, start, ref, key="orders")

    docs = []
    doc, fitted = forecast_series(spark, "All Categories", dates, values)
    docs.append(doc)
    anomalies = detect_anomalies(spark, dates, values, fitted, counts)

    cat_rows = s["lines"].filter("is_revenue").groupBy(F.col("order_date").alias("d"), "category") \
        .agg(F.sum("revenue").alias("revenue")).collect()
    for cat in sorted({r["category"] for r in cat_rows}):
        cd, cv = _series([r for r in cat_rows if r["category"] == cat], start, ref)
        cdoc, _ = forecast_series(spark, cat, cd, cv)
        cdoc.pop("history")
        cdoc["history"] = [{"date": d.isoformat(), "actual": round(float(v), 2)} for d, v in zip(cd, cv)]
        docs.append(cdoc)
    write_docs("forecasts", docs)

    # ---- order-level anomalies (Spark): unusually large orders & return abuse
    q1, q3 = orders.approxQuantile("net_amount", [0.25, 0.75], 0.001)
    fence = q3 + 3 * (q3 - q1)
    big = orders.filter(F.col("net_amount") > fence).orderBy(F.desc("net_amount")).limit(40).collect()
    for r in big:
        anomalies.append({"type": "high_value_order", "date": r["order_date"].isoformat(),
                          "order_id": r["order_id"], "customer_id": r["customer_id"],
                          "actual": r["net_amount"], "expected": round(fence, 2),
                          "payment_method": r["payment_method"], "severity": "info",
                          "direction": "spike"})
    abuse = s["orders"].groupBy("customer_id").agg(
        F.count("*").alias("orders"),
        F.sum(F.when(F.col("status") == "Returned", 1).otherwise(0)).alias("returns"),
        F.sum(F.when(F.col("status") == "Returned", F.col("net_amount")).otherwise(0)).alias("returned_value"),
    ).withColumn("return_rate", F.col("returns") / F.col("orders")) \
        .filter("returns >= 3 AND return_rate >= 0.4").orderBy(F.desc("return_rate")).limit(40).collect()
    for r in abuse:
        anomalies.append({"type": "return_abuse", "customer_id": r["customer_id"], "orders": r["orders"],
                          "returns": r["returns"], "return_rate": round(r["return_rate"] * 100, 1),
                          "actual": round(r["returned_value"], 2), "severity": "warning", "direction": "spike"})
    write_docs("anomalies", anomalies, indexes=["type", "date"])

    # validate against the generator's ground truth, when available
    truth_file = config.RAW_DIR / "_metadata.json"
    validation = None
    if truth_file.exists():
        truth = {a["date"]: a["reason"] for a in json.loads(truth_file.read_text())["planted_anomalies"]}
        found = {a["date"] for a in anomalies if a["type"] == "daily_revenue"}
        hit = [d for d in truth if d in found]
        validation = {"planted": len(truth), "detected": len(hit),
                      "recall": round(len(hit) / len(truth), 2) if truth else None,
                      "flagged_days": len(found), "details": [{"date": d, "reason": r, "detected": d in found}
                                                              for d, r in truth.items()]}

    best = docs[0]["evaluation"][0]
    get_db()["model_registry"].replace_one({"_id": "forecast_hybrid"}, {
        "_id": "forecast_hybrid", "task": "Sales Forecasting",
        "algorithm": "LinearRegression trend + RandomForestRegressor seasonality (Spark MLlib)",
        "metrics": {k: best[k] for k in ("mape", "weekly_mape", "rmse", "mae", "r2")},
        "evaluation": docs[0]["evaluation"], "holdout_days": HOLDOUT,
        "horizon_days": config.FORECAST_HORIZON_DAYS, "series": len(docs),
        "category_mape": {d["_id"]: d["evaluation"][0]["mape"] for d in docs[1:]},
        "features": CAL_FEATURES + ["t (trend)"],
        "trained_at": time.strftime("%Y-%m-%d %H:%M:%S"), "train_seconds": round(time.time() - t0, 2),
    }, upsert=True)
    get_db()["model_registry"].replace_one({"_id": "anomaly_detector"}, {
        "_id": "anomaly_detector", "task": "Anomaly Detection",
        "algorithm": "Model-residual robust z-score (MAD) + IQR fences",
        "params": {"z_threshold": 4.0, "iqr_multiplier": 3, "series": "daily orders"},
        "metrics": {"daily_anomalies": sum(a["type"] == "daily_revenue" for a in anomalies),
                    "high_value_orders": len(big), "return_abuse_customers": len(abuse),
                    **({"planted_recall": validation["recall"]} if validation else {})},
        "validation": validation, "trained_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }, upsert=True)
    return len(docs)
