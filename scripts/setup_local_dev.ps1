param(
    [string]$PythonPath = "",
    [string]$SnapshotZip = "",
    [switch]$WithNetwork
)

$ErrorActionPreference = "Stop"

function Test-PythonWithSsl {
    param(
        [string]$Command,
        [string[]]$Args = @()
    )

    try {
        $result = & $Command @($Args + @("-c", "import ssl, sys; print(sys.executable)")) 2>&1
        if ($LASTEXITCODE -ne 0) {
            return $false
        }
        $output = ($result | Out-String).Trim()
        if (-not $output) {
            return $false
        }
        if ($output -match "WindowsApps\\python\.exe") {
            return $false
        }
        return $true
    } catch {
        return $false
    }
}

function Get-PythonCandidate {
    param(
        [string]$ExplicitPath
    )

    $candidates = @()
    if ($ExplicitPath) {
        $candidates += @{ cmd = $ExplicitPath; args = @() }
    }

    $candidates += @(
        @{ cmd = "py"; args = @("-3") },
        @{ cmd = "C:\Users\mpodeley\.lmstudio\extensions\backends\vendor\_amphibian\cpython3.11-win-x86@6\python.exe"; args = @() },
        @{ cmd = "C:\Users\mpodeley\Anaconda3\python.exe"; args = @() },
        @{ cmd = "python"; args = @() }
    )

    foreach ($candidate in $candidates) {
        if (Test-PythonWithSsl -Command $candidate.cmd -Args $candidate.args) {
            return $candidate
        }
    }

    throw "No Python with SSL support was found. Install Python 3.11+ or pass -PythonPath <python.exe>."
}

function Ensure-Venv {
    param(
        [string]$RepoRoot,
        [hashtable]$Python
    )

    $venvPath = Join-Path $RepoRoot ".venv"
    $venvPython = Join-Path $venvPath "Scripts\python.exe"
    if (-not (Test-Path $venvPython)) {
        Write-Host "Creating local virtualenv at $venvPath"
        & $Python.cmd @($Python.args + @("-m", "venv", "--clear", $venvPath))
    }
    return $venvPython
}

function Install-Requirements {
    param(
        [string]$RepoRoot,
        [string]$VenvPython,
        [switch]$InstallNetwork
    )

    & $VenvPython -m pip install --upgrade pip
    & $VenvPython -m pip install -r (Join-Path $RepoRoot "requirements.txt")
    if ($InstallNetwork) {
        & $VenvPython -m pip install -r (Join-Path $RepoRoot "requirements-network.txt")
    }
}

function Expand-RuntimeSnapshot {
    param(
        [string]$RepoRoot,
        [string]$ZipPath
    )

    if (-not $ZipPath) {
        return
    }
    if (-not (Test-Path $ZipPath)) {
        throw "Snapshot zip not found at $ZipPath"
    }

    Write-Host "Expanding runtime snapshot from $ZipPath"
    Expand-Archive -Path $ZipPath -DestinationPath $RepoRoot -Force
}

function Get-RuntimeStatus {
    param(
        [string]$RepoRoot
    )

    $duckdbPath = Join-Path $RepoRoot "gas-intel-datalake\duckdb\gas_intel.duckdb"
    $processedPath = Join-Path $RepoRoot "gas-intel-datalake\data\processed"
    $dashboardPath = Join-Path $RepoRoot "gas-intel-meta\dashboard\index.html"

    $status = [ordered]@{
        duckdb = Test-Path $duckdbPath
        processed = Test-Path $processedPath
        dashboard = Test-Path $dashboardPath
    }

    return @{
        Paths = @{
            duckdb = $duckdbPath
            processed = $processedPath
            dashboard = $dashboardPath
        }
        Status = $status
    }
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Get-PythonCandidate -ExplicitPath $PythonPath

Write-Host "Repo root: $repoRoot"
Write-Host "Using Python: $($python.cmd)"
if ($python.args.Count -gt 0) {
    Write-Host "Python args: $($python.args -join ' ')"
}

$venvPython = Ensure-Venv -RepoRoot $repoRoot -Python $python
Install-Requirements -RepoRoot $repoRoot -VenvPython $venvPython -InstallNetwork:$WithNetwork
Expand-RuntimeSnapshot -RepoRoot $repoRoot -ZipPath $SnapshotZip

$runtime = Get-RuntimeStatus -RepoRoot $repoRoot

Write-Host ""
Write-Host "Local environment ready."
Write-Host "Activate with:"
Write-Host "  .\.venv\Scripts\Activate.ps1"
Write-Host ""
Write-Host "Runtime status:"
Write-Host "  DuckDB    : $($runtime.Status.duckdb) [$($runtime.Paths.duckdb)]"
Write-Host "  Processed : $($runtime.Status.processed) [$($runtime.Paths.processed)]"
Write-Host "  Dashboard : $($runtime.Status.dashboard) [$($runtime.Paths.dashboard)]"

if (-not $runtime.Status.duckdb -or -not $runtime.Status.processed) {
    Write-Host ""
    Write-Host "The code is ready, but the full runtime snapshot is incomplete."
    Write-Host "Copy or generate a snapshot and rerun:"
    Write-Host "  powershell -ExecutionPolicy Bypass -File .\scripts\setup_local_dev.ps1 -SnapshotZip C:\path\to\gcie_runtime_snapshot.zip"
}
