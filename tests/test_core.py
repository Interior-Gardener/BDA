"""Fast unit tests (no Spark / MongoDB required):  python -m pytest -q"""

import csv
import json
import math
from datetime import date, datetime
from decimal import Decimal

import numpy as np

from shopsense.datagen.generator import Generator
from shopsense.ml.forecast import _calendar, _metrics
from shopsense.mongo import clean_value


def test_generator_small(tmp_path):
    g = Generator("small", seed=1, out_dir=tmp_path)
    g.n_customers = 300                      # keep the test fast
    meta = g.run()
    assert meta["customers"] == 300
    assert (tmp_path / "orders.csv").exists()
    assert list((tmp_path / "events").glob("events_*.jsonl"))
    with open(tmp_path / "orders.csv", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == meta["orders"] > 100
    # dirty data is injected on purpose -> duplicates exist in the raw file
    assert len({r["order_id"] for r in rows}) < len(rows)
    first_event = json.loads(next(open(next((tmp_path / "events").glob("*.jsonl")), encoding="utf-8")))
    assert {"event_id", "session_id", "event_type", "event_time"} <= first_event.keys()


def test_clean_value_is_bson_safe():
    doc = clean_value({"d": date(2025, 1, 2), "t": datetime(2025, 1, 2, 3, 4), "x": Decimal("1.5"),
                       "n": float("nan"), "arr": (np.int64(3), np.float64(2.5)), "nested": {"k": [date(2024, 5, 1)]}})
    assert doc["d"] == "2025-01-02"
    assert isinstance(doc["t"], datetime)
    assert doc["x"] == 1.5 and doc["n"] is None
    assert doc["arr"] == [3, 2.5]
    assert doc["nested"]["k"] == ["2024-05-01"]


def test_calendar_features():
    diwali = _calendar(date(2025, 10, 4))       # inside "Big Festive Sale"
    assert diwali["sale_flag"] == 1 and diwali["sale_day"] == 1
    normal = _calendar(date(2025, 2, 17))
    assert normal["sale_flag"] == 0 and normal["dow"] == 0 and normal["is_salary"] == 0


def test_forecast_metrics():
    y = np.array([100.0] * 14)
    m = _metrics(y, y * 1.1)
    assert math.isclose(m["mape"], 10.0, rel_tol=1e-6)
    assert math.isclose(m["weekly_mape"], 10.0, rel_tol=1e-6)
