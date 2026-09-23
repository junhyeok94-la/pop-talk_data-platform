$ErrorActionPreference = 'Stop'
$workspace = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
& py -3 (Join-Path $workspace 'scripts/configure.py')
if ($LASTEXITCODE -ne 0) { throw 'Platform configuration failed.' }
