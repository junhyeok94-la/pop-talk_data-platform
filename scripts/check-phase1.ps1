$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
$testDatabase = 'phase1_test_' + [guid]::NewGuid().ToString('N')
$created = $false
try {
    # Read names only; never print credentials or pass them on the command line.
    $adminLine = Get-Content -LiteralPath '.env' | Where-Object { $_ -match '^POSTGRES_USER=' }
    $runtimeLine = Get-Content -LiteralPath '.local/config/airflow.env' | Where-Object { $_ -match '^POP_TALK_PLATFORM_POSTGRES_USER=' }
    $adminName = ($adminLine -split '=', 2)[1].Trim().Trim('"').Trim("'")
    $runtimeName = ($runtimeLine -split '=', 2)[1].Trim().Trim('"').Trim("'")
    if (-not $adminName -or -not $runtimeName) { throw 'Run scripts/configure.py first.' }
    docker compose exec -T db createdb -U $adminName -O $runtimeName $testDatabase
    if ($LASTEXITCODE -ne 0) { throw 'Cannot create isolated test database.' }
    $created = $true
    docker compose exec -T -e RUN_PHASE1_POSTGRES_TESTS=1 -e "POP_TALK_PLATFORM_POSTGRES_DB=$testDatabase" -e PYTHONDONTWRITEBYTECODE=1 airflow-scheduler python -m unittest tests.pipelines.platform.test_postgres_pipeline -v
    if ($LASTEXITCODE -ne 0) { throw 'Phase 1 integration tests failed. See Airflow logs/phase1-integration.' }
} finally {
    if ($created -and $testDatabase -match '^phase1_test_[0-9a-f]{32}$') {
        docker compose exec -T db dropdb -U $adminName $testDatabase
        if ($LASTEXITCODE -ne 0) { Write-Warning "Test database cleanup failed: $testDatabase" }
    }
    Pop-Location
}
