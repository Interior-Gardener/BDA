#!/usr/bin/env bash
# ShopSense - one-time setup on macOS / Linux (run from the project folder)
set -e
command -v java >/dev/null || { echo "[!] Java not found - install JDK 17 (e.g. 'brew install openjdk@17' or 'sudo apt install openjdk-17-jdk')"; exit 1; }
PY=${PYTHON:-python3.11}
command -v "$PY" >/dev/null || PY=python3
"$PY" -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
[ -f .env ] || cp .env.example .env
echo "Setup complete. Next:  ./scripts/run_unix.sh"
