# Both platforms execute the same bounded acceptance scenario.
$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
if (Test-Path '.venv\Scripts\python.exe') {
    & .\.venv\Scripts\python.exe scripts/demo.py @args
} else {
    & python scripts/demo.py @args
}
exit $LASTEXITCODE
