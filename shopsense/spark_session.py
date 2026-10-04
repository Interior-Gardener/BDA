"""SparkSession factory tuned for a laptop / single node (local mode)."""

import os
import sys

from shopsense import config


def get_spark(app_name: str = "ShopSense"):
    # Make sure Spark's Python workers use *this* interpreter and can import
    # the `shopsense` package (important on Windows and inside virtualenvs).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    root = str(config.ROOT_DIR)
    pp = os.environ.get("PYTHONPATH", "")
    if root not in pp.split(os.pathsep):
        os.environ["PYTHONPATH"] = root + (os.pathsep + pp if pp else "")

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master(config.SPARK_MASTER)
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", str(config.SPARK_SHUFFLE_PARTITIONS))
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )
    if os.getenv("MONGO_SPARK_CONNECTOR", "false").lower() in ("1", "true", "yes"):
        # Optional: official MongoDB Spark Connector (downloaded from Maven on first run)
        builder = builder.config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"
        ).config("spark.mongodb.write.connection.uri", config.MONGO_URI)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
