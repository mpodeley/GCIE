param(
    [switch]$WithNetwork
)

$ErrorActionPreference = "Stop"

function Get-PythonCommand {
    $candidates = @(
        @{ cmd = "py"; args = @("-3") },
        @{ cmd = "python"; args = @() }
    )

    foreach ($candidate in $candidates) {
        try {
            & $candidate.cmd @($candidate.args + @("--version")) | Out-Null
            return $candidate
        } catch {
        }
    }

    throw "Python 3 was not found. Install Python for your user account first."
}

$RepoRoot = Split-Path -Parent $PSScriptRoot
$VenvPath = Join-Path $RepoRoot ".venv"
$Python = Get-PythonCommand

Write-Host "Repo root: $RepoRoot"
Write-Host "Virtualenv: $VenvPath"

if (-not (Test-Path $VenvPath)) {
    Write-Host "Creating virtualenv..."
    & $Python.cmd @($Python.args + @("-m", "venv", $VenvPath))
}

$VenvPython = Join-Path $VenvPath "Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    throw "Virtualenv python was not created correctly at $VenvPython"
}

Write-Host "Upgrading pip..."
& $VenvPython -m pip install --upgrade pip

Write-Host "Installing base requirements..."
& $VenvPython -m pip install -r (Join-Path $RepoRoot "requirements.txt")

if ($WithNetwork) {
    Write-Host "Installing optional network requirements..."
    & $VenvPython -m pip install -r (Join-Path $RepoRoot "requirements-network.txt")
}

Write-Host ""
Write-Host "Bootstrap complete."
Write-Host "Activate with:"
Write-Host "  .\.venv\Scripts\Activate.ps1"
Write-Host ""
Write-Host "If PowerShell blocks activation, run commands directly with:"
Write-Host "  .\.venv\Scripts\python.exe .\gas-intel-meta\scripts\build_dashboard.py"
