# Installation Guide

This guide takes you from a fresh computer to a running ShopSense dashboard.
Total time: about **15 minutes**, plus about 7 minutes for the first pipeline run.

- [1. Install the prerequisites](#1-install-the-prerequisites)
- [2. Get the project](#2-get-the-project)
- [3. Create the Python environment](#3-create-the-python-environment)
- [4. Run the pipeline](#4-run-the-pipeline)
- [5. Start the dashboard](#5-start-the-dashboard)
- [6. Everyday commands](#6-everyday-commands)
- [Troubleshooting](#troubleshooting)

---

## 1. Install the prerequisites

| Software | Why it is needed | Check with |
|---|---|---|
| Python **3.11** | runs the pipeline, ML and web server | `python --version` |
| Java JDK **17** | Apache Spark runs on the Java Virtual Machine | `java -version` |
| MongoDB Community Server **6+** | stores all analytics results | `mongosh --eval "db.version()"` or MongoDB Compass |
| Git (optional) | clone the repository | `git --version` |

### Windows 10 / 11

1. **Python 3.11**: download from <https://www.python.org/downloads/windows/>.
   In the installer tick **"Add python.exe to PATH"**.
2. **Java 17**: download *Eclipse Temurin JDK 17 (.msi)* from <https://adoptium.net/temurin/releases/?version=17>.
   In the installer enable **"Set JAVA_HOME variable"** and **"Add to PATH"**.
   Open a *new* Command Prompt and run `java -version`. It should print `openjdk version "17..."`.
3. **MongoDB**: download *MongoDB Community Server (.msi)* from <https://www.mongodb.com/try/download/community>.
   Choose *Complete* and keep **"Install MongoDB as a Service"** checked (it then starts automatically).
   MongoDB Compass (the GUI) is optional but handy for browsing the collections.

> If you already installed Python 3.11 and MongoDB, **you only need to add Java 17**. Spark will not start without it.

### macOS (Homebrew)

```bash
brew install python@3.11 openjdk@17
sudo ln -sfn "$(brew --prefix)/opt/openjdk@17/libexec/openjdk.jdk" /Library/Java/JavaVirtualMachines/openjdk-17.jdk
brew tap mongodb/brew && brew install mongodb-community
brew services start mongodb-community
```

### Ubuntu / Debian

```bash
sudo apt update && sudo apt install -y python3.11 python3.11-venv openjdk-17-jdk
# MongoDB: follow https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-ubuntu/
sudo systemctl enable --now mongod
```

---

## 2. Get the project

```bash
git clone https://github.com/Interior-Gardener/BDA.git
cd BDA
```
(or download the ZIP from GitHub and extract it)

---

## 3. Create the Python environment

### One command

| OS | Command |
|---|---|
| Windows | `scripts\setup_windows.bat` |
| macOS / Linux | `./scripts/setup_unix.sh` |

### Manually

```bash
python -m venv .venv
# activate it
.venv\Scripts\activate          # Windows (Command Prompt)
.venv\Scripts\Activate.ps1      # Windows (PowerShell)
source .venv/bin/activate       # macOS / Linux

pip install -r requirements.txt
copy .env.example .env          # Windows   |   cp .env.example .env  on macOS / Linux
```

`requirements.txt` installs PySpark 3.5.3 (which bundles Spark itself, so no separate Spark download is needed), PyMongo, FastAPI, Uvicorn, NumPy and python-dotenv.

---

## 4. Run the pipeline

```bash
python run_pipeline.py --generate
```

What happens:

1. **Data generation**: about 315 MB of raw CSV / JSON-lines files are written to `data/raw/`.
2. **Spark ETL**: raw files are read with explicit schemas, cleaned, de-duplicated and enriched.
3. **Analytics**: KPIs, sales, funnels, cohorts and Customer 360 profiles are written to MongoDB.
4. **Machine learning**: segmentation, churn, recommendations, market basket, forecasting and anomaly detection.
5. A run log with timings and a data-quality report is saved to the `pipeline_runs` collection.

| Option | Effect |
|---|---|
| `--generate` | (re)generate the synthetic dataset first |
| `--scale small / medium / large` | dataset size (2.5 k / 10 k / 25 k customers) |
| `--skip-ml` | analytics only (about 1 minute) |

Measured run times on a 4-core laptop: *small* ≈ 5 min, *medium* ≈ 7 min, *large* ≈ 9 min. (Machine learning has a fixed cost, so small data is not much faster.)

---

## 5. Start the dashboard

```bash
python run_dashboard.py
```

- Dashboard: <http://127.0.0.1:8000>
- REST API docs (Swagger): <http://127.0.0.1:8000/docs>

Options: `--port 8050`, `--host 0.0.0.0` (to open it from another device on the same Wi-Fi), `--no-browser`, `--reload`.

---

## 6. Everyday commands

```bash
# activate the environment first (see step 3)
python run_pipeline.py            # re-run the pipeline on the existing raw data
python run_dashboard.py           # start the web app
python -m pytest -q               # run unit tests  (pip install pytest)
```

To browse the data in MongoDB Compass, connect to `mongodb://localhost:27017` and open the **`shopsense`** database.

### Optional: MongoDB Spark Connector

By default each Spark partition writes to MongoDB with PyMongo (no extra JARs needed, works offline).
To use the official connector instead, set in `.env`:

```
MONGO_SPARK_CONNECTOR=true
```
Spark downloads `org.mongodb.spark:mongo-spark-connector_2.12:10.4.0` from Maven on the first run (internet required).

### Optional: MongoDB Atlas (cloud)

Put your Atlas connection string in `.env`:
```
MONGO_URI=mongodb+srv://<user>:<password>@<cluster>.mongodb.net
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `JAVA_HOME is not set` / `Java gateway process exited before sending its port number` | Java missing or not on PATH | Install JDK 17, set `JAVA_HOME` to its folder (e.g. `C:\Program Files\Eclipse Adoptium\jdk-17...`), open a new terminal |
| `UnsupportedClassVersionError` | Java 8 is being used | Install JDK 17 and make sure `java -version` shows 17 |
| `Cannot reach MongoDB at mongodb://localhost:27017` | MongoDB service stopped | Windows: `services.msc` → *MongoDB Server* → Start · macOS: `brew services start mongodb-community` · Linux: `sudo systemctl start mongod` |
| `Python worker failed to connect back` | Spark picked another Python | Activate `.venv` before running; ShopSense sets `PYSPARK_PYTHON` to the current interpreter |
| `WARN Shell: Did not find winutils.exe` | Hadoop helper missing on Windows | Harmless: Parquet output is disabled on Windows by default |
| `Exception while deleting Spark temp dir` at exit (Windows) | Windows file locking | Harmless, can be ignored |
| `java.lang.OutOfMemoryError: Java heap space` | Not enough driver memory | `--scale small`, or `SPARK_DRIVER_MEMORY=2g` / `4g` in `.env` |
| `[Errno 10048]` / `address already in use` | Port 8000 busy | `python run_dashboard.py --port 8050` |
| Dashboard shows "Could not load this page" | Pipeline not run or MongoDB down | Run `python run_pipeline.py`; check MongoDB |
| `pip install` fails building PySpark | Old `setuptools` | `python -m pip install --upgrade pip setuptools wheel`, then retry |
