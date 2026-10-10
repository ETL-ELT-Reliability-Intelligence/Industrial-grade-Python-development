# Start PostgreSQL, install dependencies and apply migrations (Windows, Python 3.12).
# Run from anywhere: powershell -File scripts/storage_setup.ps1
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

function Invoke-Checked {
    param([string]$Command, [string[]]$Arguments)
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$Command failed with exit code $LASTEXITCODE" }
}

Invoke-Checked docker @("compose", "-f", "infra/docker-compose.yml", "up", "-d", "--wait", "postgres")
Invoke-Checked py @("-3.12", "-m", "pip", "install", "-e", ".[dev]", "-r", "storage/requirements.txt")

if (-not $env:DATABASE_URL) {
    $env:DATABASE_URL = "postgresql://reliability:reliability@localhost:5432/reliability"
}
Invoke-Checked py @("-3.12", "-m", "alembic", "-c", "storage/postgres/alembic.ini", "upgrade", "head")
Write-Host "Storage is ready: $env:DATABASE_URL"
