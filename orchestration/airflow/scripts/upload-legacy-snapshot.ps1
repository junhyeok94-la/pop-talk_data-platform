param(
    [Parameter(Mandatory=$true)][string]$SourcePath
)
$ErrorActionPreference = 'Stop'
$snapshotBytes = [System.IO.File]::ReadAllBytes((Resolve-Path -LiteralPath $SourcePath).Path)
Push-Location ((Resolve-Path (Join-Path $PSScriptRoot "../../..")).Path)
try {
    [Convert]::ToBase64String($snapshotBytes) | docker --config .docker-local compose exec -T airflow-scheduler python /opt/airflow/scripts/upload_legacy_snapshot.py
    if ($LASTEXITCODE -ne 0) { throw 'Legacy snapshot upload or verification failed' }
} finally { Pop-Location }
