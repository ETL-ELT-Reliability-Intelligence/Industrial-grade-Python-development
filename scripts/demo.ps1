# End-to-end demo: Olist tables -> ingestion -> S3 + Kafka -> PostgreSQL raw -> dbt -> quality decisions -> dashboard.
#
# Three normal monthly runs build the history, the fourth silently loses 60% of order_payments while every
# pipeline step still succeeds. The quality bridge must catch it: BLOCK on the dashboard, and the revenue
# built by dbt for that month is visibly too low.
#
# Needs only Docker. Real Olist CSVs in data\olist are used if present, otherwise a small synthetic set in
# the same format is generated. POSTGRES_PORT (default 5433) avoids a clash with a local PostgreSQL on 5432.
#
# Not "Stop": in Windows PowerShell 5.1 any stderr output of a native command (docker prints progress there)
# would be turned into a terminating error. Exit codes are checked explicitly instead.
$ErrorActionPreference = "Continue"
Set-Location (Join-Path $PSScriptRoot "..")

if (-not $env:POSTGRES_PORT) { $env:POSTGRES_PORT = "5433" }
$stamp = [int][double]::Parse((Get-Date -UFormat %s))
$runTimeout = if ($env:RUN_TIMEOUT) { [int]$env:RUN_TIMEOUT } else { 600 }

function Assert-Ok([string]$What) {
    if ($LASTEXITCODE -ne 0) { Write-Error "$What failed (exit code $LASTEXITCODE)"; exit 1 }
}

function Invoke-Psql([string[]]$Arguments) {
    docker compose exec -T postgres psql -U reliability -d reliability @Arguments
}

function Invoke-Run([string]$Label, [string]$Suffix, [string]$Conf) {
    Write-Host "== $Label"
    $runId = "demo-$stamp-$Suffix"
    # Windows PowerShell 5.1 drops double quotes inside arguments of native commands; escape them.
    $arg = $Conf -replace '"', '\"'
    docker compose exec -T airflow airflow dags trigger olist_ingestion --run-id $runId --conf $arg 2>&1 | Out-Null
    Assert-Ok "triggering the DAG"
    $state = ""; $waited = 0
    while ($waited -lt $runTimeout) {
        $state = ((docker compose exec -T airflow airflow dags state olist_ingestion $runId 2>&1 | Out-String).Trim() -split "`n" | Select-Object -Last 1).Trim()
        if ($state -eq "success" -or $state -eq "failed") { break }
        Start-Sleep 5; $waited += 5
    }
    Write-Host "   state=$state (${waited}s)"
    if ($state -ne "success") {
        docker compose exec -T airflow airflow tasks states-for-dag-run olist_ingestion $runId 2>&1 | Select-Object -Last 15
        Write-Error "DAG run '$Label' did not succeed (state='$state'). See http://localhost:8080/dags/olist_ingestion/grid"
        exit 1
    }
}

if (-not (Test-Path "data\olist\olist_orders_dataset.csv")) {
    Write-Host "== data\olist is empty: generating synthetic Olist-format data"
    python -m services.ingestion.olist_sample data/olist --months 4 --orders-per-month 300
}

Write-Host "== starting infrastructure"
docker compose up -d --wait postgres
Assert-Ok "docker compose up postgres"
docker compose up -d kafka rustfs migrate backend stream-processor kafka-ui adminer airflow airflow-scheduler
Assert-Ok "docker compose up"

Write-Host "== waiting for Airflow (the first start builds the image: a few minutes)"
do { Start-Sleep 5; docker compose exec -T airflow curl -sf localhost:8080/health 2>&1 | Out-Null } until ($LASTEXITCODE -eq 0)
do { Start-Sleep 3; $dags = docker compose exec -T airflow airflow dags list 2>&1 | Out-String } until ($dags -match "olist_ingestion")
docker compose exec -T airflow airflow dags unpause olist_ingestion 2>&1 | Out-Null

Invoke-Run "month 1 (normal)" 1 '{"window": 0}'
Invoke-Run "month 2 (normal)" 2 '{"window": 1}'
Invoke-Run "month 3 (normal)" 3 '{"window": 2}'
Invoke-Run "month 4: upstream silently returns 40% of order_payments" 4 '{"window": 3, "volume_factors": {"order_payments": 0.4}}'

Write-Host "== waiting for the quality bridge to judge the last batches"
for ($i = 0; $i -lt 30; $i++) {
    $n = (Invoke-Psql @("-tAc", "select count(*) from quality_decisions where dataset_id = 'order_payments'") 2>&1 | Out-String).Trim()
    if ($n -match "^\d+$" -and [int]$n -ge 4) { break }
    Start-Sleep 2
}

Write-Host ""
Write-Host "================ S3 (raw data lake) ================"
docker compose exec -T airflow python /opt/airflow/repo/scripts/show_s3.py
Write-Host ""
Write-Host "================ PostgreSQL ================"
Get-Content scripts\demo_postgres.sql | docker compose exec -T postgres psql -U reliability -d reliability

Write-Host @"

================ Where to look ================
  Dashboard (quality)  http://localhost:8000                 order_payments, last batch -> BLOCK
  Airflow              http://localhost:8080                 admin / admin   (local demo only)
  Kafka UI             http://localhost:8081                 topics -> dataset.ingested
  PostgreSQL (Adminer) http://localhost:8082/?pgsql=postgres&username=reliability&db=reliability
                                                             system PostgreSQL, user/password reliability
  S3 console (RustFS)  http://localhost:9001                 minioadmin / minioadmin, bucket raw-data
  Stop and remove everything: docker compose down -v
"@
