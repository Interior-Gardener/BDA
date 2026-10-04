"""ShopSense - run the complete Big Data pipeline.

Examples
    python run_pipeline.py                  # use existing raw data (generates if missing)
    python run_pipeline.py --generate       # regenerate the synthetic dataset first
    python run_pipeline.py --generate --scale small
    python run_pipeline.py --skip-ml        # analytics only
"""

import argparse
import sys

for stream in (sys.stdout, sys.stderr):
    try:
        stream.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass

from shopsense.pipeline import run_pipeline  # noqa: E402

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Run the ShopSense Spark + MongoDB + ML pipeline")
    ap.add_argument("--generate", action="store_true", help="(re)generate the synthetic raw dataset")
    ap.add_argument("--scale", choices=["small", "medium", "large"], help="dataset size for --generate")
    ap.add_argument("--skip-ml", action="store_true", help="skip the machine-learning stages")
    a = ap.parse_args()
    ok = run_pipeline(generate=a.generate, scale=a.scale, skip_ml=a.skip_ml)
    sys.exit(0 if ok else 1)
