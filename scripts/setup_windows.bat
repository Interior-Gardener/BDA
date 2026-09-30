@echo off
REM ShopSense - one-time setup on Windows (run from the project folder)
echo === ShopSense setup ===
where java >nul 2>nul || (echo [!] Java not found. Install JDK 17 from https://adoptium.net and re-run. & exit /b 1)
where python >nul 2>nul || (echo [!] Python not found. Install Python 3.11 and re-run. & exit /b 1)
python -m venv .venv
call .venv\Scripts\activate.bat
python -m pip install --upgrade pip
pip install -r requirements.txt
if not exist .env copy .env.example .env
echo.
echo Setup complete. Next:  scripts\run_windows.bat
