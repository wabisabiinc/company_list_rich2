$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

if (Test-Path ".venv/Scripts/Activate.ps1") {
  . .venv/Scripts/Activate.ps1
}

if (-not $env:UPDATE_CHECK_ENABLED) { $env:UPDATE_CHECK_ENABLED = "true" }
if (-not $env:UPDATE_CHECK_FINAL_ONLY) { $env:UPDATE_CHECK_FINAL_ONLY = "true" }
if (-not $env:UPDATE_CHECK_TIMEOUT_MS) { $env:UPDATE_CHECK_TIMEOUT_MS = "2500" }
if (-not $env:UPDATE_CHECK_ALLOW_SLOW) { $env:UPDATE_CHECK_ALLOW_SLOW = "false" }
if (-not $env:MAX_ROWS) { $env:MAX_ROWS = "0" }

python .\main.py
