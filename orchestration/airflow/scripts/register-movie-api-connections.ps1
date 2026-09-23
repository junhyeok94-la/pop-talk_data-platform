[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$SourceEnv
)
$ErrorActionPreference = 'Stop'
$project = (Resolve-Path (Join-Path $PSScriptRoot "../../..")).Path
$selected = @{}
foreach ($line in Get-Content -LiteralPath $SourceEnv) {
    if ($line -match '^\s*(?:export\s+)?(KOFIC_API_KEY|KMDB_API_KEY)\s*=\s*(.*)$') {
        $name, $value = $matches[1], $matches[2].Trim()
        if ($value.StartsWith('"') -or $value.StartsWith("'")) {
            $quote = $value.Substring(0, 1)
            $end = $value.IndexOf($quote, 1)
            if ($end -lt 1) { throw "Invalid quoted value for $name" }
            $value = $value.Substring(1, $end - 1)
        } else {
            $value = ($value -replace '\s+#.*$', '').Trim()
        }
        # Legacy supports CSV keys; use only the first, without quota rotation.
        $selected[$name] = ($value -split ',')[0].Trim()
    }
}
foreach ($name in @('KOFIC_API_KEY','KMDB_API_KEY')) {
    if (-not $selected[$name]) { throw "Missing $name in source env" }
}
Push-Location $project
try {
    # Secrets go only through stdin, never command-line arguments or a temporary file.
    $selected | ConvertTo-Json -Compress | docker --config .docker-local compose exec -T airflow-scheduler python /opt/airflow/scripts/register_movie_api_connections.py
    if ($LASTEXITCODE -ne 0) { throw 'Airflow Connection registration failed' }
} finally {
    $selected.Clear()
    Pop-Location
}
