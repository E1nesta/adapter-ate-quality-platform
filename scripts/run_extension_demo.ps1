[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

$VenvPython = Join-Path $RepoRoot ".venv-win\Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    throw "Windows virtual environment not found. Run scripts\bootstrap_demo.ps1 first."
}

& (Join-Path $PSScriptRoot "run_mvp_demo.ps1")

& $VenvPython -m adapter_ate.ai_model `
    --processed-dir "data\processed" `
    --model "models\quality_model.joblib" `
    --metrics "reports\model_metrics.json"

& $VenvPython "scripts\api_smoke.py"

if ($env:MYSQL_HOST) {
    & $VenvPython -m adapter_ate.storage `
        --processed-dir "data\processed" `
        --create-schema
}
else {
    Write-Host "MYSQL_HOST is not set; skipping MySQL import"
}

Write-Host "Extension demo complete"
