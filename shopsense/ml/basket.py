"""Market-basket analysis with FP-Growth (Spark MLlib).

Mines association rules  {A} -> {B}  at two levels:
  * product level     - concrete "frequently bought together" bundles
  * sub-category level - merchandising insights (e.g. Smartphones -> Accessories)

support    = P(A and B)            (share of all baskets containing both)
confidence = P(B | A)              (of baskets with A, share that also have B)
lift       = confidence / P(B)     (> 1 means a genuine positive association)
"""

import time

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import functions as F

from shopsense.mongo import bulk_update, get_db, write_docs


def _mine(baskets, min_support, min_conf):
    fp = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_conf)
    model = fp.fit(baskets)
    rules = model.associationRules.filter("lift > 1").orderBy(F.desc("lift")).limit(400).collect()
    freq = model.freqItemsets.count()
    return rules, freq


def run(spark, s, ref):
    t0 = time.time()
    lines = s["lines"].filter(F.col("status") != "Cancelled")
    baskets_p = lines.groupBy("order_id").agg(F.collect_set("product_id").alias("items")) \
        .filter(F.size("items") >= 1).cache()
    n_baskets = baskets_p.count()
    multi = baskets_p.filter(F.size("items") >= 2).count()
    baskets_s = lines.groupBy("order_id").agg(F.collect_set("sub_category").alias("items")).cache()

    prod_rules, prod_freq = _mine(baskets_p, 0.0008, 0.05)
    sub_rules, sub_freq = _mine(baskets_s, 0.002, 0.05)

    info = {r["product_id"]: r for r in s["products"].select(
        "product_id", "product_name", "category", "sub_category", "list_price").collect()}

    docs = []
    for level, rules in (("product", prod_rules), ("sub_category", sub_rules)):
        for r in rules:
            a, c = list(r["antecedent"]), list(r["consequent"])
            d = {"level": level, "antecedent": a, "consequent": c,
                 "support": round(r["support"], 5), "confidence": round(r["confidence"], 4),
                 "lift": round(r["lift"], 3)}
            if level == "product":
                d["antecedent_names"] = [info[x]["product_name"] for x in a]
                d["consequent_names"] = [info[x]["product_name"] for x in c]
                d["antecedent_category"] = info[a[0]]["sub_category"]
                d["consequent_category"] = info[c[0]]["sub_category"]
            docs.append(d)
    write_docs("basket_rules", docs, indexes=["level", "antecedent", [("lift", True)]])

    # "frequently bought together" list on each product document
    fbt = {}
    for r in prod_rules:
        if len(r["antecedent"]) == 1:
            fbt.setdefault(r["antecedent"][0], []).append(
                {"product_id": r["consequent"][0], "confidence": round(r["confidence"], 4),
                 "lift": round(r["lift"], 2)})
    bulk_update("products", [(k, {"bought_together": sorted(v, key=lambda x: -x["confidence"])[:5]})
                             for k, v in fbt.items()])

    get_db()["model_registry"].replace_one({"_id": "basket_fpgrowth"}, {
        "_id": "basket_fpgrowth", "task": "Market Basket Analysis", "algorithm": "FP-Growth (Spark MLlib)",
        "params": {"product": {"minSupport": 0.0008, "minConfidence": 0.05},
                   "sub_category": {"minSupport": 0.002, "minConfidence": 0.05}},
        "metrics": {"baskets": n_baskets, "multi_item_baskets": multi,
                    "multi_item_share_pct": round(multi / n_baskets * 100, 2),
                    "product_frequent_itemsets": prod_freq, "product_rules": len(prod_rules),
                    "subcategory_frequent_itemsets": sub_freq, "subcategory_rules": len(sub_rules)},
        "trained_at": time.strftime("%Y-%m-%d %H:%M:%S"), "train_seconds": round(time.time() - t0, 2),
    }, upsert=True)
    baskets_p.unpersist()
    baskets_s.unpersist()
    return len(docs)
