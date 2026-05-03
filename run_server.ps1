$ErrorActionPreference = "Stop"

$Python = "C:\Program Files\Python313\python.exe"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path

Set-Location $Root
& $Python -m uvicorn bot:app --host 127.0.0.1 --port 8080

