# Run the whole test suite, including PostgreSQL integration tests (Windows, Python 3.12).
# Requires a running database: powershell -File scripts/storage_setup.ps1
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

if (-not $env:TEST_DATABASE_URL) {
    $env:TEST_DATABASE_URL = "postgresql://reliability:reliability@localhost:5432/reliability_test"
}
py -3.12 -m pytest -q
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
