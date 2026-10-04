"""Central configuration. Values can be overridden through environment
variables or a `.env` file placed in the project root."""

import os
import platform
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # python-dotenv is optional
    load_dotenv = None

ROOT_DIR = Path(__file__).resolve().parent.parent

if load_dotenv:
    load_dotenv(ROOT_DIR / ".env")

# ---------------------------------------------------------------- MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "shopsense")

# ---------------------------------------------------------------- Storage
DATA_DIR = Path(os.getenv("DATA_DIR", ROOT_DIR / "data"))
RAW_DIR = DATA_DIR / "raw"          # landing zone (CSV / JSON-lines)
LAKE_DIR = DATA_DIR / "lake"        # optional Parquet data lake (silver layer)
DATA_SCALE = os.getenv("DATA_SCALE", "medium").lower()

# ---------------------------------------------------------------- Spark
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "3g")
SPARK_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))

IS_WINDOWS = platform.system() == "Windows"


def save_parquet_enabled() -> bool:
    """Writing Parquet on Windows needs Hadoop's winutils.exe, so by default
    it is only enabled on Linux/macOS or when HADOOP_HOME is configured."""
    flag = os.getenv("SAVE_PARQUET", "auto").lower()
    if flag in ("1", "true", "yes", "on"):
        return True
    if flag in ("0", "false", "no", "off"):
        return False
    return (not IS_WINDOWS) or bool(os.getenv("HADOOP_HOME"))


# ---------------------------------------------------------------- Analytics
CHURN_WINDOW_DAYS = 90       # "no purchase in the next 90 days" = churned
FORECAST_HORIZON_DAYS = 60
TOP_N_RECOMMENDATIONS = 10

# ---------------------------------------------------------------- Web
API_HOST = os.getenv("API_HOST", "127.0.0.1")
API_PORT = int(os.getenv("API_PORT", "8000"))
