[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

function Invoke-PythonLauncher {
    param([string[]]$Arguments)

    if (Get-Command py -ErrorAction SilentlyContinue) {
        & py -3 @Arguments
        return
    }

    if (Get-Command python -ErrorAction SilentlyContinue) {
        & python @Arguments
        return
    }

    throw "Python 3 is required. Install Python 3.10+ and make py or python available in PATH."
}

function Import-DotEnv {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        return
    }

    Get-Content $Path | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) {
            return
        }

        $name, $value = $line.Split("=", 2)
        [Environment]::SetEnvironmentVariable($name.Trim(), $value.Trim(), "Process")
    }
}

if (-not (Test-Path ".venv-win")) {
    Invoke-PythonLauncher -Arguments @("-m", "venv", ".venv-win")
}

$VenvPython = Join-Path $RepoRoot ".venv-win\Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    throw "Failed to create .venv-win."
}

& $VenvPython -m pip install -r requirements.txt

Import-DotEnv -Path (Join-Path $RepoRoot ".env")

if (-not $env:ATE_DATA_SOURCE) {
    $env:ATE_DATA_SOURCE = "csv"
}

& (Join-Path $PSScriptRoot "run_extension_demo.ps1")
