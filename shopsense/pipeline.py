"""End-to-end batch pipeline orchestrator.

    raw files ──► Spark ingest (bronze) ──► clean/enrich (silver) ──► analytics (gold)
                                                             └──► MLlib models ──► MongoDB

Every stage is timed; the run log, data-quality report and dataset stats are
stored in the `pipeline_runs` collection and shown on the dashboard.
"""

import json
import platform
import time
import traceback
import uuid
from datetime import datetime

from shopsense import config
from shopsense.analytics import behaviour, sales
from shopsense.etl.clean import clean
from shopsense.etl.ingest import read_raw
from shopsense.ml import basket, churn, forecast, recommender, segmentation
from shopsense.mongo import get_db, ping
from shopsense.spark_session import get_spark

BANNER = r"""
   _____ _                 _____
  / ____| |               / ____|
 | (___ | |__   ___  _ __| (___   ___ _ __  ___  ___
  \___ \| '_ \ / _ \| '_ \\___ \ / _ \ '_ \/ __|/ _ \
  ____) | | | | (_) | |_) |___) |  __/ | | \__ \  __/
 |_____/|_| |_|\___/| .__/_____/ \___|_| |_|___/\___|
                    | |   E-Commerce Sales & Customer Behaviour Analytics
                    |_|   Spark + MongoDB + Machine Learning
"""


class Run:
    def __init__(self):
        self.id = datetime.now().strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:4]
        self.stages = []
        self.t0 = time.time()

    def stage(self, group, name, fn, *args):
        print(f"  ▶ {name:<44}", end="", flush=True)
        t = time.time()
        try:
            out = fn(*args)
            dt = time.time() - t
            self.stages.append({"group": group, "name": name, "seconds": round(dt, 2),
                                "status": "success", "records": out if isinstance(out, int) else None})
            print(f"✔ {dt:6.1f}s" + (f"   ({out:,} records)" if isinstance(out, int) else ""))
            return out
        except Exception as e:
            dt = time.time() - t
            self.stages.append({"group": group, "name": name, "seconds": round(dt, 2),
                                "status": "failed", "error": str(e)[:500]})
            print(f"✘ {dt:6.1f}s")
            raise


def run_pipeline(generate=False, scale=None, skip_ml=False):
    print(BANNER)
    try:
        ping()
    except Exception as e:
        raise SystemExit(f"Cannot reach MongoDB at {config.MONGO_URI}: {e}\n"
                         "Start MongoDB (mongod) or set MONGO_URI in .env")
    run = Run()
    db = get_db()
    status = "success"
    dq, ref, spark = {}, None, None
    try:
        if generate or not (config.RAW_DIR / "orders.csv").exists():
            from shopsense.datagen.generator import generate as gen
            print("● Data generation")
            run.stage("Generate", "Synthetic dataset", lambda: gen(scale or config.DATA_SCALE)["events"])

        print("● Spark session")
        spark = run.stage("Setup", "Start SparkSession", lambda: get_spark("ShopSense-Pipeline"))

        print("● Ingest & clean  (bronze → silver)")
        bronze = run.stage("ETL", "Read raw CSV / JSON-lines (bronze)", read_raw, spark)
        silver, dq, ref = run.stage("ETL", "Clean, validate, de-duplicate, enrich", clean, spark, bronze)

        print("● Analytics  (silver → gold → MongoDB)")
        run.stage("Analytics", "KPIs", sales.kpis, spark, silver, ref)
        run.stage("Analytics", "Daily / monthly sales", sales.daily_and_monthly, spark, silver)
        run.stage("Analytics", "Category / geo / payment breakdowns", sales.breakdowns, spark, silver)
        run.stage("Analytics", "Sale campaign uplift", sales.campaigns, spark, silver)
        run.stage("Analytics", "Product performance", sales.product_stats, spark, silver)
        run.stage("Analytics", "Conversion funnel", behaviour.funnel, spark, silver)
        run.stage("Analytics", "Traffic & sessions", behaviour.traffic, spark, silver)
        run.stage("Analytics", "Cohort retention", behaviour.cohorts, spark, silver)
        run.stage("Analytics", "Orders collection (embedded items)", behaviour.orders_collection, spark, silver)
        run.stage("Analytics", "Customer 360 profiles", behaviour.customer_profiles, spark, silver, ref)

        if not skip_ml:
            print("● Machine learning  (Spark MLlib)")
            run.stage("ML", "Segmentation · K-Means + RFM", segmentation.run, spark, silver, ref)
            run.stage("ML", "Churn · LR / RF / GBT", churn.run, spark, silver, ref)
            run.stage("ML", "Recommendations · ALS", recommender.run, spark, silver, ref)
            run.stage("ML", "Market basket · FP-Growth", basket.run, spark, silver, ref)
            run.stage("ML", "Forecast + anomaly detection", forecast.run, spark, silver, ref)
    except Exception:
        status = "failed"
        traceback.print_exc()
    finally:
        meta = {}
        mf = config.RAW_DIR / "_metadata.json"
        if mf.exists():
            meta = json.loads(mf.read_text())
        colls = {c: db[c].estimated_document_count() for c in sorted(db.list_collection_names())
                 if c != "pipeline_runs"}
        spark_info = {}
        if spark is not None:
            sc = spark.sparkContext
            spark_info = {"version": spark.version, "master": sc.master,
                          "default_parallelism": sc.defaultParallelism,
                          "driver_memory": config.SPARK_DRIVER_MEMORY,
                          "shuffle_partitions": config.SPARK_SHUFFLE_PARTITIONS}
        doc = {"_id": run.id, "started_at": datetime.fromtimestamp(run.t0),
               "finished_at": datetime.now(), "duration_sec": round(time.time() - run.t0, 2),
               "status": status, "stages": run.stages, "data_quality": dq,
               "as_of": ref.isoformat() if ref else None, "dataset": meta, "collections": colls,
               "spark": spark_info, "platform": f"{platform.system()} {platform.release()} · "
                                                f"Python {platform.python_version()}"}
        db["pipeline_runs"].insert_one(doc)
        if spark is not None:
            spark.stop()
    print(f"\n{'✔' if status == 'success' else '✘'} Pipeline {status} in {time.time() - run.t0:.1f}s  "
          f"(run id {run.id})")
    if status == "success":
        print(f"  Start the dashboard:  python run_dashboard.py   →  http://{config.API_HOST}:{config.API_PORT}")
    return status == "success"
