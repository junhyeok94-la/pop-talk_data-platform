param(
    [int]$Port = 11434
)

$ErrorActionPreference = 'Stop'

$baseUrl = "http://127.0.0.1:$Port"
$ollamaExe = Join-Path $env:LOCALAPPDATA 'Programs\Ollama\ollama.exe'

try {
    $tags = Invoke-RestMethod "$baseUrl/api/tags" -TimeoutSec 5
}
catch {
    throw "Ollama is not reachable at $baseUrl. Run orchestration\airflow\scripts\start-local-ollama.ps1 first."
}

$names = @($tags.models | ForEach-Object { $_.name })
foreach ($required in @('qwen3:8b', 'bge-m3:latest')) {
    if ($required -notin $names) {
        throw "Required model is missing from the active Ollama server: $required"
    }
}

Write-Host '[1/2] Testing BGE-M3 embedding...'
$embedInputBase64 = '7KCg642w7J207JWE6rCAIOy2nOyXsO2VnCAyMDIw64WEIOydtO2bhCDsmIHtmZQ='
$embedInput = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($embedInputBase64))
$embedBody = @{
    model = 'bge-m3'
    input = $embedInput
} | ConvertTo-Json

$timer = [Diagnostics.Stopwatch]::StartNew()
$embedBodyBytes = [Text.Encoding]::UTF8.GetBytes($embedBody)
$embed = Invoke-RestMethod -Method Post -Uri "$baseUrl/api/embed" -ContentType 'application/json; charset=utf-8' -Body $embedBodyBytes -TimeoutSec 180
$timer.Stop()
$dimensions = $embed.embeddings[0].Count

if ($dimensions -ne 1024) {
    throw "Unexpected BGE-M3 dimensions: $dimensions"
}

Write-Host "BGE-M3 OK: dimensions=$dimensions elapsed_ms=$($timer.ElapsedMilliseconds)"

# Keep only one large workload active while testing the 12GB GPU.
$unloadEmbedding = @{ model = 'bge-m3'; keep_alive = 0; stream = $false } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "$baseUrl/api/generate" -ContentType 'application/json' -Body $unloadEmbedding -TimeoutSec 30 | Out-Null

Write-Host '[2/2] Testing Qwen3 Korean generation...'
$promptBase64 = '7ZWc6rWt7Ja066GcIOygle2Zle2eiCDtlZwg66y47J6l66eMIOuLte2VmOyEuOyalC4g7JiB7ZmUIOuNsOydtO2EsOybqOyWtO2VmOyasOyKpOydmCDsl63tlaDsnYAg66y07JeH7J6F64uI6rmMPw=='
$prompt = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($promptBase64))
$chatBody = @{
    model = 'qwen3:8b'
    messages = @(
        @{
            role = 'user'
            content = $prompt
        }
    )
    stream = $false
    think = $false
    options = @{
        num_predict = 100
        temperature = 0
    }
} | ConvertTo-Json -Depth 6

$timer.Restart()
$chatBodyBytes = [Text.Encoding]::UTF8.GetBytes($chatBody)
$chat = Invoke-RestMethod -Method Post -Uri "$baseUrl/api/chat" -ContentType 'application/json; charset=utf-8' -Body $chatBodyBytes -TimeoutSec 300
$timer.Stop()
$answer = $chat.message.content.Trim()

if ([string]::IsNullOrWhiteSpace($answer)) {
    throw 'Qwen3 returned an empty answer.'
}

if ($answer -notmatch '[\uAC00-\uD7A3]') {
    throw "Qwen3 did not return a valid Korean answer: $answer"
}

Write-Host "Qwen3 OK: elapsed_ms=$($timer.ElapsedMilliseconds)"
Write-Host "Answer: $answer"

$env:OLLAMA_HOST = $baseUrl
Write-Host 'GPU/model status:'
& $ollamaExe ps
