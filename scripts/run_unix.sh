#!/usr/bin/env bash
# ShopSense - run the Spark pipeline (first time) and start the dashboard
set -e
source .venv/bin/activate
if [ ! -f data/raw/orders.csv ]; then
  python run_pipeline.py --generate
elif [ "$1" == "--pipeline" ]; then
  python run_pipeline.py
fi
python run_dashboard.py
