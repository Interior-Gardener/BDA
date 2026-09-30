@echo off
REM ShopSense - run the Spark pipeline (first time) and start the dashboard
call .venv\Scripts\activate.bat
if not exist data\raw\orders.csv (
  python run_pipeline.py --generate
) else if "%1"=="--pipeline" (
  python run_pipeline.py
)
python run_dashboard.py
