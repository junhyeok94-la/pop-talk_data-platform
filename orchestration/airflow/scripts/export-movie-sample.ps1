[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [string]$SourcePath,

    [Parameter()]
    [ValidateRange(1, 10000)]
    [int]$SampleSize = 100,

    [Parameter()]
    [string]$OutputRoot = ".local/data/dw-samples"
)

$ErrorActionPreference = "Stop"

$resolvedSource = (Resolve-Path -LiteralPath $SourcePath).Path
$sourceDocument = Get-Content -Raw -LiteralPath $resolvedSource | ConvertFrom-Json
$movies = @($sourceDocument.movies | Sort-Object -Property id)

if ($movies.Count -lt $SampleSize) {
    throw "Source contains $($movies.Count) movies, fewer than requested sample size $SampleSize."
}

$createdAt = [DateTimeOffset]::UtcNow
$runId = "movie-sample-$($createdAt.ToString('yyyyMMddTHHmmssZ'))"
$runDirectory = Join-Path $OutputRoot $runId
New-Item -ItemType Directory -Path $runDirectory -Force | Out-Null

$dataFile = Join-Path $runDirectory "movies.jsonl"
$manifestFile = Join-Path $runDirectory "manifest.json"
$selectedMovies = @($movies | Select-Object -First $SampleSize)

$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
$jsonLines = $selectedMovies | ForEach-Object {
    $_ | ConvertTo-Json -Depth 20 -Compress
}
[System.IO.File]::WriteAllLines((Join-Path (Get-Location) $dataFile), $jsonLines, $utf8NoBom)

$checksum = (Get-FileHash -Algorithm SHA256 -LiteralPath $dataFile).Hash.ToLowerInvariant()
$sourceChecksum = (Get-FileHash -Algorithm SHA256 -LiteralPath $resolvedSource).Hash.ToLowerInvariant()
$ids = @($selectedMovies | ForEach-Object { [int64]$_.id })

$manifest = [ordered]@{
    manifest_version = 1
    run_id = $runId
    source = "pop_talk_admin_snapshot"
    source_path = Split-Path -Leaf $resolvedSource
    source_captured_at = $sourceDocument.captured_at
    created_at_utc = $createdAt.ToString("o")
    selection = [ordered]@{
        method = "lowest_movie_id"
        requested_count = $SampleSize
        actual_count = $selectedMovies.Count
        minimum_movie_id = ($ids | Measure-Object -Minimum).Minimum
        maximum_movie_id = ($ids | Measure-Object -Maximum).Maximum
    }
    data_file = [ordered]@{
        name = "movies.jsonl"
        format = "json_lines"
        encoding = "utf-8"
        sha256 = $checksum
        bytes = (Get-Item -LiteralPath $dataFile).Length
    }
    source_file_sha256 = $sourceChecksum
}

$manifestJson = $manifest | ConvertTo-Json -Depth 10
[System.IO.File]::WriteAllText((Join-Path (Get-Location) $manifestFile), $manifestJson, $utf8NoBom)

[pscustomobject]@{
    RunId = $runId
    MovieCount = $selectedMovies.Count
    DataFile = (Resolve-Path -LiteralPath $dataFile).Path
    ManifestFile = (Resolve-Path -LiteralPath $manifestFile).Path
    DataSha256 = $checksum
}
