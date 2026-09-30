"""ShopSense REST API + dashboard server (FastAPI).

All heavy lifting happens offline in the Spark pipeline; this service only
queries the pre-computed *gold* collections in MongoDB (plus a few live
MongoDB aggregation pipelines for date / category filters), so every
dashboard interaction responds in milliseconds.

Run:  python run_dashboard.py      (interactive API docs at /docs)
"""

import csv
import io
import math
import re
from datetime import datetime
from pathlib import Path

from bson import ObjectId
from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from shopsense import __version__, config
from shopsense.mongo import get_db

STATIC = Path(__file__).parent / "static"

app = FastAPI(
    title="ShopSense API",
    version=__version__,
    description="E-Commerce Sales & Customer Behaviour Analytics — Spark + MongoDB + Machine Learning",
)
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory=STATIC), name="static")


def db():
    return get_db()


def _fix(obj):
    if isinstance(obj, list):
        return [_fix(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _fix(v) for k, v in obj.items()}
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


def find(coll, q=None, proj=None, sort=None, limit=0):
    cur = db()[coll].find(q or {}, {"_id": 0, **(proj or {})})
    if sort:
        cur = cur.sort(sort)
    if limit:
        cur = cur.limit(limit)
    return _fix(list(cur))


def one(coll, q, proj=None):
    doc = db()[coll].find_one(q, proj)
    return _fix(doc) if doc else None


def _date_match(start, end):
    m = {}
    if start:
        m["$gte"] = start
    if end:
        m["$lte"] = end
    return {"date": m} if m else {}


# ------------------------------------------------------------------ pages
@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/health", tags=["System"])
def health():
    try:
        db().command("ping")
        runs = db()["pipeline_runs"].count_documents({})
        return {"status": "ok", "mongo": "connected", "database": config.MONGO_DB,
                "pipeline_runs": runs, "ready": db()["kpis"].count_documents({}) > 0}
    except Exception as e:  # pragma: no cover
        return {"status": "error", "mongo": str(e), "ready": False}


@app.get("/api/meta", tags=["System"])
def meta():
    k = one("kpis", {"_id": "global"}) or {}
    first = db()["sales_daily"].find_one(sort=[("date", 1)])
    last = db()["sales_daily"].find_one(sort=[("date", -1)])
    return {
        "as_of": k.get("as_of"),
        "min_date": first["date"] if first else None,
        "max_date": last["date"] if last else None,
        "categories": sorted(db()["sales_by_category"].distinct("category")),
        "segments": [s["segment"] for s in find("segments", proj={"segment": 1})],
        "forecast_series": [d["_id"] for d in db()["forecasts"].find({}, {"_id": 1})],
        "version": __version__,
    }


# ------------------------------------------------------------------ overview
@app.get("/api/overview", tags=["Overview"])
def overview():
    k = one("kpis", {"_id": "global"})
    if not k:
        raise HTTPException(503, "No data yet - run `python run_pipeline.py` first.")
    fc = one("forecasts", {"_id": "All Categories"}, {"next_30_days": 1, "last_30_days": 1}) or {}
    return {
        "kpis": k,
        "monthly": find("sales_monthly", sort=[("month", 1)]),
        "categories": find("sales_by_category", sort=[("revenue", -1)]),
        "top_products": find("products", proj={"similar": 0, "bought_together": 0},
                             sort=[("revenue", -1)], limit=6),
        "segments": find("segments", proj={"segment": 1, "customers": 1, "revenue": 1, "revenue_share_pct": 1}),
        "funnel": one("funnel", {"dimension": "all"}, {"_id": 0}),
        "anomalies": find("anomalies", {"type": "daily_revenue"}, sort=[("date", -1)], limit=5),
        "forecast": fc,
        "churn": one("churn_summary", {"_id": "summary"},
                     {"risk_distribution": 1, "revenue_at_risk_12m": 1, "best_model": 1}),
    }


# ------------------------------------------------------------------ sales
@app.get("/api/sales/trend", tags=["Sales"])
def sales_trend(start: str = None, end: str = None, category: str = None,
                granularity: str = Query("day", pattern="^(day|week|month)$")):
    """Revenue time-series computed live with a MongoDB aggregation pipeline."""
    coll = "sales_daily_category" if category else "sales_daily"
    match = _date_match(start, end)
    if category:
        match["category"] = category
    if granularity == "day":
        key = "$date"
    elif granularity == "month":
        key = {"$substr": ["$date", 0, 7]}
    else:
        key = {"$dateToString": {"format": "%Y-%m-%d", "date": {
            "$dateTrunc": {"date": {"$dateFromString": {"dateString": "$date"}}, "unit": "week",
                           "startOfWeek": "monday"}}}}
    pipe = [{"$match": match},
            {"$group": {"_id": key, "revenue": {"$sum": "$revenue"}, "orders": {"$sum": "$orders"},
                        "units": {"$sum": "$units"}, "profit": {"$sum": "$profit"},
                        "dates": {"$addToSet": "$date"}}},
            {"$sort": {"_id": 1}}]
    rows = list(db()[coll].aggregate(pipe))
    out = [{"period": r["_id"], "revenue": round(r["revenue"], 2), "orders": r["orders"],
            "units": r["units"], "profit": round(r["profit"], 2), "days": len(r["dates"]),
            "aov": round(r["revenue"] / r["orders"], 2) if r["orders"] else 0} for r in rows]
    tot_rev = sum(r["revenue"] for r in out)
    tot_ord = sum(r["orders"] for r in out)
    return {"series": out, "totals": {"revenue": round(tot_rev, 2), "orders": tot_ord,
                                      "profit": round(sum(r["profit"] for r in out), 2),
                                      "units": sum(r["units"] for r in out),
                                      "aov": round(tot_rev / tot_ord, 2) if tot_ord else 0}}


@app.get("/api/sales/categories", tags=["Sales"])
def sales_categories(start: str = None, end: str = None):
    pipe = [{"$match": _date_match(start, end)},
            {"$group": {"_id": "$category", "revenue": {"$sum": "$revenue"}, "units": {"$sum": "$units"},
                        "profit": {"$sum": "$profit"}, "orders": {"$sum": "$orders"}}},
            {"$sort": {"revenue": -1}}]
    live = {r["_id"]: r for r in db()["sales_daily_category"].aggregate(pipe)}
    static = {d["category"]: d for d in find("sales_by_category")}
    total = sum(r["revenue"] for r in live.values()) or 1
    out = []
    for cat, r in live.items():
        s = static.get(cat, {})
        out.append({"category": cat, "revenue": round(r["revenue"], 2), "units": r["units"],
                    "orders": r["orders"], "profit": round(r["profit"], 2),
                    "margin_pct": round(r["profit"] / r["revenue"] * 100, 2) if r["revenue"] else 0,
                    "share_pct": round(r["revenue"] / total * 100, 2),
                    "return_rate": s.get("return_rate"), "avg_rating": s.get("avg_rating"),
                    "avg_discount_pct": s.get("avg_discount_pct")})
    return {"categories": out, "subcategories": find("sales_by_subcategory", sort=[("revenue", -1)])}


@app.get("/api/sales/geo", tags=["Sales"])
def sales_geo():
    return {"cities": find("sales_by_city", sort=[("revenue", -1)]),
            "states": find("sales_by_state", sort=[("revenue", -1)])}


@app.get("/api/sales/dimensions", tags=["Sales"])
def sales_dimensions():
    out = {}
    for d in find("sales_by_dimension"):
        out.setdefault(d["dimension"], []).append(d)
    for v in out.values():
        v.sort(key=lambda x: -(x.get("revenue") or 0))
    return out


@app.get("/api/sales/heatmap", tags=["Sales"])
def sales_heatmap():
    return find("sales_heatmap")


@app.get("/api/sales/monthly", tags=["Sales"])
def sales_monthly():
    return find("sales_monthly", sort=[("month", 1)])


@app.get("/api/campaigns", tags=["Sales"])
def campaigns():
    return find("campaigns", sort=[("start", 1)])


# ------------------------------------------------------------------ products
PRODUCT_SORTS = {"revenue", "units", "avg_rating", "return_rate", "views", "view_to_buy_pct", "profit", "list_price"}


@app.get("/api/products", tags=["Products"])
def products(category: str = None, q: str = None, sort: str = "revenue", order: str = "desc",
             limit: int = Query(25, le=200), skip: int = 0):
    if sort not in PRODUCT_SORTS:
        sort = "revenue"
    match = {}
    if category:
        match["category"] = category
    if q:
        match["product_name"] = {"$regex": re.escape(q), "$options": "i"}
    total = db()["products"].count_documents(match)
    rows = find("products", match, {"similar": 0, "bought_together": 0},
                sort=[(sort, -1 if order == "desc" else 1)], limit=limit + skip)[skip:]
    return {"total": total, "items": rows}


@app.get("/api/products/{product_id}", tags=["Products"])
def product(product_id: str):
    p = one("products", {"_id": product_id})
    if not p:
        raise HTTPException(404, "Product not found")
    ids = [x["product_id"] for x in p.get("similar", [])] + [x["product_id"] for x in p.get("bought_together", [])]
    info = {d["product_id"]: d for d in find("products", {"product_id": {"$in": ids}},
                                               {"product_id": 1, "product_name": 1, "category": 1,
                                                "sub_category": 1, "list_price": 1, "avg_rating": 1})}
    for key in ("similar", "bought_together"):
        p[key] = [{**x, **info.get(x["product_id"], {})} for x in p.get(key, [])]
    return p


# ------------------------------------------------------------------ behaviour
@app.get("/api/behaviour/funnel", tags=["Customer Behaviour"])
def funnel():
    out = {"all": None, "device": [], "traffic_source": []}
    for d in find("funnel"):
        if d["dimension"] == "all":
            out["all"] = d
        else:
            out[d["dimension"]].append(d)
    return out


@app.get("/api/behaviour/traffic", tags=["Customer Behaviour"])
def traffic():
    return {"hourly": find("traffic_hourly", sort=[("hour", 1)]),
            "monthly": find("traffic_monthly", sort=[("month", 1)]),
            "events": find("event_counts", sort=[("count", -1)])}


@app.get("/api/behaviour/cohorts", tags=["Customer Behaviour"])
def cohorts():
    return find("cohorts", sort=[("cohort", 1)])


# ------------------------------------------------------------------ segmentation
@app.get("/api/segments", tags=["Segmentation"])
def segments():
    return {"segments": find("segments", sort=[("revenue", -1)]),
            "points": find("segment_points"),
            "model": one("model_registry", {"_id": "segmentation_kmeans"})}


CUSTOMER_LIST = {"customer_id": 1, "full_name": 1, "city": 1, "segment": 1, "orders": 1, "total_spent": 1,
                 "recency_days": 1, "churn_probability": 1, "churn_risk": 1, "clv_12m": 1,
                 "favourite_category": 1, "rfm": 1, "email": 1, "last_order": 1}


@app.get("/api/segments/{name}/customers", tags=["Segmentation"])
def segment_customers(name: str, limit: int = Query(50, le=500)):
    return find("customers", {"segment": name}, CUSTOMER_LIST, sort=[("total_spent", -1)], limit=limit)


# ------------------------------------------------------------------ churn
@app.get("/api/churn/summary", tags=["Churn"])
def churn_summary():
    s = one("churn_summary", {"_id": "summary"})
    if not s:
        raise HTTPException(503, "Churn model not trained yet")
    exp = one("model_registry", {"_id": "churn_logistic_export"}) or {}
    s["simulator"] = {"numeric": exp.get("numeric", []), "labels": exp.get("labels", {}),
                      "categorical": exp.get("categorical", {}), "defaults": _feature_medians()}
    by_seg = list(db()["customers"].aggregate([
        {"$match": {"churn_probability": {"$ne": None}}},
        {"$group": {"_id": "$segment", "avg": {"$avg": "$churn_probability"}, "n": {"$sum": 1},
                    "high": {"$sum": {"$cond": [{"$eq": ["$churn_risk", "High"]}, 1, 0]}}}},
        {"$sort": {"avg": -1}}]))
    s["by_segment"] = [{"segment": r["_id"], "avg_probability": round(r["avg"], 4), "customers": r["n"],
                        "high_risk": r["high"]} for r in by_seg]
    return s


def _feature_medians():
    exp = db()["model_registry"].find_one({"_id": "churn_logistic_export"})
    if not exp:
        return {}
    out = {}
    for f in exp["numeric"]:
        vals = sorted(d["churn_features"][f] for d in db()["customers"].find(
            {"churn_features": {"$exists": True}}, {f"churn_features.{f}": 1}).limit(3000)
            if d.get("churn_features", {}).get(f) is not None)
        out[f] = vals[len(vals) // 2] if vals else 0
    return out


def _churn_logit(features):
    exp = db()["model_registry"].find_one({"_id": "churn_logistic_export"})
    if not exp:
        raise HTTPException(503, "Churn model not trained yet")
    values = {}
    for f in exp["numeric"]:
        v = float(features.get(f, 0) or 0)
        values[f"log_{f}" if f in exp["log_features"] else f] = math.log1p(max(v, 0)) if f in exp["log_features"] else v
    for cat, labels in exp["categorical"].items():
        chosen = features.get(cat)
        for lab in labels:
            values[f"{cat}_ohe_{lab}"] = 1.0 if chosen == lab else 0.0
    contrib = []
    logit = exp["intercept"]
    for name, mu, sd, w in zip(exp["feature_names"], exp["mean"], exp["std"], exp["coef"]):
        z = (values.get(name, 0.0) - mu) / sd if sd else 0.0
        c = w * z
        logit += c
        base = name.replace("log_", "")
        label = exp["labels"].get(base) or base.replace("_ohe_", ": ").replace("_", " ").title()
        contrib.append({"feature": label, "key": base, "contribution": round(c, 4)})
    prob = 1 / (1 + math.exp(-logit))
    contrib.sort(key=lambda x: -abs(x["contribution"]))
    return prob, contrib


@app.post("/api/churn/simulate", tags=["Churn"])
def churn_simulate(features: dict = Body(..., example={"recency_days": 120, "frequency": 3, "monetary": 9000,
                                                       "preferred_device": "Mobile App"})):
    """What-if analysis with the exported (explainable) logistic-regression churn model."""
    prob, contrib = _churn_logit(features)
    return {"probability": round(prob, 4),
            "risk": "High" if prob >= 0.7 else ("Medium" if prob >= 0.4 else "Low"),
            "drivers": contrib[:8], "model": "Logistic Regression (exported from Spark MLlib)"}


@app.get("/api/churn/customers", tags=["Churn"])
def churn_customers(risk: str = "High", segment: str = None, limit: int = Query(50, le=1000),
                    sort: str = "clv_12m"):
    q = {"churn_risk": risk} if risk and risk != "All" else {"churn_probability": {"$ne": None}}
    if segment:
        q["segment"] = segment
    if sort not in ("clv_12m", "churn_probability", "total_spent", "recency_days"):
        sort = "clv_12m"
    return find("customers", q, CUSTOMER_LIST, sort=[(sort, -1)], limit=limit)


@app.get("/api/churn/customers.csv", tags=["Churn"])
def churn_customers_csv(risk: str = "High", segment: str = None):
    """Export a win-back campaign list (CSV) for the marketing team."""
    rows = churn_customers(risk, segment, 1000, "clv_12m")
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["customer_id", "name", "email", "city", "segment", "orders", "total_spent",
                "days_since_last_order", "churn_probability", "risk", "predicted_12m_value", "favourite_category"])
    for r in rows:
        w.writerow([r.get("customer_id"), r.get("full_name"), r.get("email"), r.get("city"), r.get("segment"),
                    r.get("orders"), r.get("total_spent"), r.get("recency_days"), r.get("churn_probability"),
                    r.get("churn_risk"), r.get("clv_12m"), r.get("favourite_category")])
    buf.seek(0)
    name = f"shopsense_winback_{risk.lower()}_{datetime.now():%Y%m%d}.csv"
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": f"attachment; filename={name}"})


# ------------------------------------------------------------------ recommendations & basket
@app.get("/api/recommendations/{customer_id}", tags=["Recommendations"])
def recommendations(customer_id: str):
    r = one("recommendations", {"_id": customer_id})
    if not r:
        r = one("recommendations", {"_id": "__popular__"}) or {"items": []}
        r["fallback"] = True
    history = list(db()["orders"].aggregate([
        {"$match": {"customer_id": customer_id, "status": {"$ne": "Cancelled"}}},
        {"$sort": {"order_ts": -1}}, {"$limit": 15}, {"$unwind": "$items"},
        {"$group": {"_id": "$items.product_id", "product_name": {"$first": "$items.product_name"},
                    "category": {"$first": "$items.category"}, "last": {"$max": "$order_ts"}}},
        {"$sort": {"last": -1}}, {"$limit": 8}]))
    r["purchased"] = _fix(history)
    r["model"] = one("model_registry", {"_id": "recommender_als"})
    return r


@app.get("/api/basket/rules", tags=["Market Basket"])
def basket_rules(level: str = "product", min_lift: float = 1.0, q: str = None, limit: int = Query(50, le=400)):
    match = {"level": level, "lift": {"$gte": min_lift}}
    if q:
        rx = {"$regex": re.escape(q), "$options": "i"}
        match["$or"] = [{"antecedent_names": rx}, {"consequent_names": rx}, {"antecedent": rx}, {"consequent": rx}]
    return {"rules": find("basket_rules", match, sort=[("lift", -1)], limit=limit),
            "model": one("model_registry", {"_id": "basket_fpgrowth"})}


# ------------------------------------------------------------------ forecast & anomalies
@app.get("/api/forecast", tags=["Forecasting"])
def forecast(series: str = "All Categories", history_days: int = 240):
    d = one("forecasts", {"_id": series})
    if not d:
        raise HTTPException(404, "Unknown series")
    d["history"] = d["history"][-history_days:]
    d["model"] = one("model_registry", {"_id": "forecast_hybrid"})
    d["all_series"] = [{"series": x["_id"], "mape": x["evaluation"][0]["mape"], "next_30_days": x["next_30_days"],
                        "last_30_days": x["last_30_days"]}
                       for x in db()["forecasts"].find({}, {"evaluation": 1, "next_30_days": 1, "last_30_days": 1})]
    return d


@app.get("/api/anomalies", tags=["Anomalies"])
def anomalies(type: str = None):
    q = {"type": type} if type else {}
    return {"items": find("anomalies", q, sort=[("date", -1)], limit=200),
            "model": one("model_registry", {"_id": "anomaly_detector"})}


# ------------------------------------------------------------------ customer 360
@app.get("/api/customers/search", tags=["Customer 360"])
def customer_search(q: str = "", limit: int = 10):
    q = q.strip()
    if not q:
        return find("customers", {"segment": "Champions"}, CUSTOMER_LIST, sort=[("total_spent", -1)], limit=limit)
    rx = {"$regex": re.escape(q.lower())}
    cond = {"$or": [{"customer_id": {"$regex": "^" + re.escape(q.upper())}}, {"search_name": rx}]}
    return find("customers", cond, CUSTOMER_LIST, sort=[("total_spent", -1)], limit=limit)


@app.get("/api/customers/{customer_id}", tags=["Customer 360"])
def customer(customer_id: str):
    c = one("customers", {"_id": customer_id})
    if not c:
        raise HTTPException(404, "Customer not found")
    c["orders_recent"] = find("orders", {"customer_id": customer_id}, sort=[("order_ts", -1)], limit=12)
    monthly = list(db()["orders"].aggregate([
        {"$match": {"customer_id": customer_id, "status": {"$nin": ["Cancelled", "Returned"]}}},
        {"$group": {"_id": {"$substr": ["$order_date", 0, 7]}, "revenue": {"$sum": "$net_amount"},
                    "orders": {"$sum": 1}}}, {"$sort": {"_id": 1}}]))
    c["monthly_spend"] = [{"month": m["_id"], "revenue": round(m["revenue"], 2), "orders": m["orders"]}
                          for m in monthly]
    cats = list(db()["orders"].aggregate([
        {"$match": {"customer_id": customer_id, "status": {"$ne": "Cancelled"}}}, {"$unwind": "$items"},
        {"$group": {"_id": "$items.category", "revenue": {"$sum": "$items.revenue"}}}, {"$sort": {"revenue": -1}}]))
    c["category_spend"] = [{"category": x["_id"], "revenue": round(x["revenue"], 2)} for x in cats]
    rec = one("recommendations", {"_id": customer_id})
    c["recommendations"] = (rec or {}).get("items", [])
    if c.get("churn_features"):
        try:
            prob, contrib = _churn_logit(c["churn_features"])
            c["churn_drivers"] = contrib[:6]
        except HTTPException:
            pass
    seg = one("segments", {"_id": c.get("segment")}, {"action": 1})
    c["segment_action"] = (seg or {}).get("action")
    return c


# ------------------------------------------------------------------ pipeline & models
@app.get("/api/pipeline/runs", tags=["System"])
def pipeline_runs(limit: int = 10):
    runs = _fix(list(db()["pipeline_runs"].find().sort("started_at", -1).limit(limit)))
    return {"runs": runs, "latest": runs[0] if runs else None}


@app.get("/api/models", tags=["System"])
def models():
    return find("model_registry", {"_id": {"$ne": "churn_logistic_export"}},
                proj={"_id": 1, "task": 1, "algorithm": 1, "metrics": 1, "trained_at": 1, "train_seconds": 1,
                      "params": 1, "k": 1, "silhouette": 1, "is_best": 1, "best_params": 1})
