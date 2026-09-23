param(
    [int]$Port = 11434
)

$ErrorActionPreference = 'Stop'

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "../../..")).Path
$modelRoot = Join-Path $projectRoot '.local\models\ollama'
$ollamaExe = Join-Path $env:LOCALAPPDATA 'Programs\Ollama\ollama.exe'
$baseUrl = "http://127.0.0.1:$Port"

if (-not (Test-Path -LiteralPath $ollamaExe)) {
    throw "Ollama executable not found: $ollamaExe"
}

if (-not (Test-Path -LiteralPath $modelRoot)) {
    throw "D drive model cache not found: $modelRoot"
}

$existing = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
if ($existing) {
    try {
        $tags = Invoke-RestMethod "$baseUrl/api/tags" -TimeoutSec 5
        Write-Host "Ollama is already listening on port $Port. PID=$($existing.OwningProcess)"
        $tags.models | Select-Object name, size | Format-Table -AutoSize
        exit 0
    }
    catch {
        throw "Port $Port is already occupied by PID $($existing.OwningProcess), but it is not a healthy Ollama server."
    }
}

$env:OLLAMA_MODELS = $modelRoot
$env:OLLAMA_HOST = "127.0.0.1:$Port"

$process = Start-Process -FilePath $ollamaExe -ArgumentList @('serve') -WindowStyle Hidden -PassThru

for ($attempt = 1; $attempt -le 30; $attempt++) {
    Start-Sleep -Seconds 1
    try {
        $tags = Invoke-RestMethod "$baseUrl/api/tags" -TimeoutSec 2
        Write-Host "Ollama started. PID=$($process.Id) URL=$baseUrl"
        Write-Host "Model cache: $modelRoot"
        $tags.models | Select-Object name, size | Format-Table -AutoSize
        exit 0
    }
    catch {
        if ($process.HasExited) {
            throw "Ollama exited before becoming ready. ExitCode=$($process.ExitCode)"
        }
    }
}

throw "Ollama did not become ready within 30 seconds."

