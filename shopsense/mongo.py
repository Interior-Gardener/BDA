"""MongoDB helpers: connection, BSON-safe conversion and a distributed
DataFrame writer (each Spark partition bulk-inserts its own rows)."""

import math
import os
from datetime import date, datetime
from decimal import Decimal

from pymongo import ASCENDING, DESCENDING, MongoClient

from shopsense import config

_client = None


def get_client():
    global _client
    if _client is None:
        _client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
    return _client


def get_db():
    return get_client()[config.MONGO_DB]


def ping():
    get_client().admin.command("ping")
    return True


def clean_value(v):
    """Convert Spark/Python values into BSON-friendly types."""
    if v is None:
        return None
    if hasattr(v, "asDict"):                       # pyspark Row
        return {k: clean_value(x) for k, x in v.asDict().items()}
    if isinstance(v, dict):
        return {str(k): clean_value(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [clean_value(x) for x in v]
    if isinstance(v, datetime):
        return v.replace(tzinfo=None)
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, float):
        return None if (math.isnan(v) or math.isinf(v)) else v
    if hasattr(v, "toArray"):                      # pyspark ml Vector
        return [float(x) for x in v.toArray()]
    if hasattr(v, "item"):                         # numpy scalar
        return v.item()
    return v


def to_doc(row, id_col=None):
    d = clean_value(row)
    if id_col is not None:
        d["_id"] = d[id_col]
    return d


def write_docs(collection, docs, drop=True, indexes=None):
    """Write a list of python dicts (driver side)."""
    coll = get_db()[collection]
    if drop:
        coll.drop()
    docs = [clean_value(d) for d in docs]
    for i in range(0, len(docs), 5000):
        coll.insert_many(docs[i:i + 5000], ordered=False)
    _create_indexes(coll, indexes)
    return len(docs)


def write_df(df, collection, id_col=None, drop=True, indexes=None):
    """Distributed write of a Spark DataFrame into MongoDB.

    Uses the official MongoDB Spark Connector when MONGO_SPARK_CONNECTOR=true,
    otherwise every executor partition opens its own pymongo connection and
    bulk-inserts its rows (no extra JARs needed - works offline and on Windows).
    """
    coll = get_db()[collection]
    if drop:
        coll.drop()
    if os.getenv("MONGO_SPARK_CONNECTOR", "false").lower() in ("1", "true", "yes"):
        out = df.withColumn("_id", df[id_col]) if id_col else df
        (out.write.format("mongodb").mode("append")
            .option("database", config.MONGO_DB).option("collection", collection).save())
    else:
        uri, dbname = config.MONGO_URI, config.MONGO_DB

        def _write_partition(rows):
            from pymongo import MongoClient as _MC
            from shopsense.mongo import to_doc as _to_doc
            client = _MC(uri)
            target = client[dbname][collection]
            batch = []
            for r in rows:
                batch.append(_to_doc(r, id_col))
                if len(batch) >= 2000:
                    target.insert_many(batch, ordered=False)
                    batch = []
            if batch:
                target.insert_many(batch, ordered=False)
            client.close()

        df.foreachPartition(_write_partition)
    _create_indexes(coll, indexes)
    return coll.estimated_document_count()


def _create_indexes(coll, indexes):
    for idx in indexes or []:
        if isinstance(idx, str):
            coll.create_index([(idx, ASCENDING)])
        else:
            coll.create_index([(f, DESCENDING if desc else ASCENDING) for f, desc in idx])


def bulk_update(collection, updates, key="_id"):
    """updates: iterable of (id, {field: value}) -> $set in batches."""
    from pymongo import UpdateOne
    coll = get_db()[collection]
    ops = [UpdateOne({key: k}, {"$set": clean_value(v)}) for k, v in updates]
    for i in range(0, len(ops), 5000):
        coll.bulk_write(ops[i:i + 5000], ordered=False)
    return len(ops)
