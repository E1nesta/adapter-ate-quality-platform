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

$BuildDir = Join-Path $RepoRoot "build"
$RawDir = Join-Path $RepoRoot "data\raw"
$ProcessedDir = Join-Path $RepoRoot "data\processed"
$ReportsDir = Join-Path $RepoRoot "reports"
$ModelsDir = Join-Path $RepoRoot "models"

New-Item -ItemType Directory -Force -Path $BuildDir, $RawDir, $ProcessedDir, $ReportsDir, $ModelsDir | Out-Null

$Source = Join-Path $RepoRoot "cpp_ate_simulator\ate_line_simulator.cpp"
$Simulator = Join-Path $BuildDir "ate_line_simulator.exe"

if (Get-Command cl.exe -ErrorAction SilentlyContinue) {
    & cl.exe /nologo /std:c++17 /EHsc /W4 "/Fe:$Simulator" $Source
}
elseif (Get-Command g++ -ErrorAction SilentlyContinue) {
    & g++ -std=c++17 -Wall -Wextra $Source -o $Simulator
}
else {
    throw "C++ compiler not found. Install Visual Studio Build Tools or MinGW g++."
}

& $Simulator `
    --count 100 `
    --output-dir "data\raw" `
    --seed 20260425 `
    --abnormal-rate 0.2 `
    --batch-no B20260425 `
    --product-model ADP-65W `
    --line-id LINE-01

& $VenvPython -m adapter_ate.processor `
    --raw-dir "data\raw" `
    --config "config\test_rules.json" `
    --output-dir "data\processed" `
    --log "reports\process.log"

& $VenvPython -m adapter_ate.reports `
    --processed-dir "data\processed" `
    --reports-dir "reports"

Write-Host "MVP demo complete"
Write-Host "Processed results: data\processed\processed_results.csv"
Write-Host "Reports: reports\daily_summary.csv reports\batch_summary.csv reports\defect_summary.csv"
