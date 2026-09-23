$ErrorActionPreference = 'Stop'
$workspace = (Resolve-Path (Join-Path $PSScriptRoot "../../..")).Path
$airflowEnv = Join-Path $workspace '.local/config/airflow.env'
$workerEnv = Join-Path $workspace '.local/config/model-worker.env'
if (-not (Test-Path -LiteralPath $airflowEnv)) { throw 'Run prepare-airflow.ps1 first.' }
if (Test-Path -LiteralPath $workerEnv) {
    $line = Get-Content -LiteralPath $workerEnv | Where-Object { $_.StartsWith('MODEL_WORKER_TOKEN=') } | Select-Object -First 1
    $token = $line.Substring('MODEL_WORKER_TOKEN='.Length)
} else {
    $token = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)).ToLowerInvariant()
    "MODEL_WORKER_TOKEN=$token" | Set-Content -LiteralPath $workerEnv -Encoding utf8
}
$values = Get-Content -LiteralPath $airflowEnv
if (-not ($values | Where-Object { $_.StartsWith('AIRFLOW_CONN_WORKBENCH_MODEL_WORKER=') })) {
    $connection = @{conn_type='http';host='http://model-worker:8091';password=$token} | ConvertTo-Json -Compress
    "AIRFLOW_CONN_WORKBENCH_MODEL_WORKER=$connection" | Add-Content -LiteralPath $airflowEnv -Encoding utf8
}
foreach ($relative in @('.local/data/model-worker','.local/models/huggingface')) {
    New-Item -ItemType Directory -Force -Path (Join-Path $workspace $relative) | Out-Null
}
Write-Host 'Model Lab worker credentials and workspace prepared. Existing secrets kept.'
