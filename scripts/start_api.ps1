[CmdletBinding()]
param(
    [string]$HostName = "127.0.0.1",
    [int]$Port = 5000,
    [string]$ProcessedDir = "data\processed",
    [string]$ReportsDir = "reports",
    [string]$Model = "models\quality_model.joblib"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

$VenvPython = Join-Path $RepoRoot ".venv-win\Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    throw "Windows virtual environment not found. Run scripts\bootstrap_demo.ps1 first."
}

if (-not $env:ATE_DATA_SOURCE) {
    $env:ATE_DATA_SOURCE = "csv"
}

& $VenvPython -m adapter_ate.api `
    --processed-dir $ProcessedDir `
    --reports-dir $ReportsDir `
    --model $Model `
    --host $HostName `
    --port $Port
