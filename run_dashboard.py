"""ShopSense - start the web dashboard + REST API.

    python run_dashboard.py            ->  http://127.0.0.1:8000
    API documentation (Swagger UI)     ->  http://127.0.0.1:8000/docs
"""

import argparse
import webbrowser
from threading import Timer

import uvicorn

from shopsense import config

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Run the ShopSense dashboard")
    ap.add_argument("--host", default=config.API_HOST)
    ap.add_argument("--port", type=int, default=config.API_PORT)
    ap.add_argument("--no-browser", action="store_true", help="do not open a browser tab")
    ap.add_argument("--reload", action="store_true", help="auto-reload on code changes (development)")
    a = ap.parse_args()
    url = f"http://{a.host}:{a.port}"
    print(f"\n  ShopSense dashboard  ->  {url}\n  REST API docs        ->  {url}/docs\n")
    if not a.no_browser:
        Timer(1.5, lambda: webbrowser.open(url)).start()
    uvicorn.run("app.main:app", host=a.host, port=a.port, reload=a.reload, log_level="warning")
