@echo off
cd /d "%~dp0"
"C:\Program Files\Python313\python.exe" -m uvicorn bot:app --host 127.0.0.1 --port 8080

