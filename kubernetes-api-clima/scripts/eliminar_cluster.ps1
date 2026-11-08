$ErrorActionPreference = 'Stop'
$kind = Join-Path (Resolve-Path (Join-Path $PSScriptRoot '..')) 'tools\kind.exe'
& $kind delete cluster --name clima-ecuador

