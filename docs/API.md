# REST API Reference

Base URL: `http://127.0.0.1:8000` · interactive Swagger UI: **`/docs`** · OpenAPI JSON: `/openapi.json`

All responses are JSON (gzip-compressed) unless stated otherwise. Monetary values are in Indian Rupees (₹). Dates are ISO `YYYY-MM-DD` strings.

## System

| Method | Path | Description |
|---|---|---|
| GET | `/api/health` | MongoDB connectivity and whether pipeline data exists |
| GET | `/api/meta` | data date range, category list, segment names, forecast series |
| GET | `/api/pipeline/runs?limit=10` | latest pipeline runs (stage timings, data-quality report, collection counts) |
| GET | `/api/models` | model registry (algorithm, parameters, metrics) |

## Overview & sales

| Method | Path | Parameters | Description |
|---|---|---|---|
| GET | `/api/overview` | – | KPIs, monthly trend, categories, top products, segments, funnel, anomalies, forecast, churn summary |
| GET | `/api/sales/trend` | `start`, `end`, `category`, `granularity` = `day` \| `week` \| `month` | revenue / orders / units / profit series computed **live** with a MongoDB aggregation pipeline |
| GET | `/api/sales/categories` | `start`, `end` | category revenue, share and margin for the period + sub-category totals |
| GET | `/api/sales/geo` | – | revenue by city (with coordinates) and state |
| GET | `/api/sales/dimensions` | – | revenue, orders, cancel & return rate by payment method, device, city tier, region, acquisition channel |
| GET | `/api/sales/heatmap` | – | orders by day-of-week × hour |
| GET | `/api/sales/monthly` | – | monthly revenue, new vs returning customers, MoM growth |
| GET | `/api/campaigns` | – | order and revenue uplift of every sale event |

Example:
```bash
curl "http://127.0.0.1:8000/api/sales/trend?start=2026-01-01&end=2026-06-30&category=Fashion&granularity=month"
```
```json
{"series": [{"period": "2026-01", "revenue": 3016405.4, "orders": 906, "units": 1296, "profit": 1094951.81, "days": 31, "aov": 3329.37}, "..."],
 "totals": {"revenue": 19734603.36, "orders": 5603, "profit": 7324028.17, "units": 8132, "aov": 3522.15}}
```

## Products

| Method | Path | Parameters | Description |
|---|---|---|---|
| GET | `/api/products` | `category`, `q` (name search), `sort` = revenue \| units \| avg_rating \| return_rate \| views \| view_to_buy_pct \| profit \| list_price, `order` = desc \| asc, `limit` (≤ 200), `skip` | product KPIs |
| GET | `/api/products/{product_id}` | – | product + `similar[]` (ALS) + `bought_together[]` (FP-Growth) |

## Customer behaviour

| Method | Path | Description |
|---|---|---|
| GET | `/api/behaviour/funnel` | funnel for all sessions, per device and per traffic source |
| GET | `/api/behaviour/traffic` | sessions by hour and month, event-type counts |
| GET | `/api/behaviour/cohorts` | monthly first-purchase cohorts with retention % |

## Machine learning

| Method | Path | Parameters | Description |
|---|---|---|---|
| GET | `/api/segments` | – | segments, PCA points, K-Means model info |
| GET | `/api/segments/{name}/customers` | `limit` | top customers of a segment |
| GET | `/api/churn/summary` | – | model comparison, ROC curves, importances, risk distribution, simulator metadata |
| POST | `/api/churn/simulate` | JSON body of feature values | what-if churn probability + drivers |
| GET | `/api/churn/customers` | `risk` = High \| Medium \| Low \| All, `segment`, `limit`, `sort` | risk list |
| GET | `/api/churn/customers.csv` | `risk`, `segment` | **CSV download** for a win-back campaign |
| GET | `/api/recommendations/{customer_id}` | – | Top-10 ALS recommendations + recent purchases + model metrics |
| GET | `/api/basket/rules` | `level` = product \| sub_category, `min_lift`, `q`, `limit` | association rules |
| GET | `/api/forecast` | `series` (e.g. `All Categories`, `Electronics`), `history_days` | history, fit, forecast with 80/95% intervals, evaluation |
| GET | `/api/anomalies` | `type` = daily_revenue \| high_value_order \| return_abuse | anomalies + detector validation |

What-if example:
```bash
curl -X POST http://127.0.0.1:8000/api/churn/simulate \
     -H "Content-Type: application/json" \
     -d '{"recency_days": 150, "frequency": 4, "monetary": 18000, "sessions_30d": 0,
          "sessions_90d": 1, "days_since_last_session": 60, "preferred_device": "Mobile App"}'
```
```json
{"probability": 0.9405, "risk": "High",
 "drivers": [{"feature": "Spend in last 90 days", "key": "spend_90d", "contribution": 0.4091}, "..."],
 "model": "Logistic Regression (exported from Spark MLlib)"}
```
Unspecified numeric features default to 0. Missing categorical values count as "not in any known category".

## Customer 360

| Method | Path | Parameters | Description |
|---|---|---|---|
| GET | `/api/customers/search` | `q` (name or ID prefix), `limit` | search; an empty `q` returns top Champions |
| GET | `/api/customers/{customer_id}` | – | full profile: RFM, churn score & drivers, CLV, monthly spend, category mix, 12 recent orders (with embedded items), recommendations |

## Errors

| Status | Meaning |
|---|---|
| 404 | unknown customer / product / forecast series |
| 422 | invalid query parameter (e.g. `granularity=year`) |
| 503 | the pipeline has not been run yet (no data in MongoDB) |
