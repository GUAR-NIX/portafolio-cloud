$ErrorActionPreference = 'Stop'
$tools = Join-Path $PSScriptRoot '..\tools'
$python = Join-Path $PSScriptRoot '..\.venv\Scripts\python.exe'
if (-not (Test-Path $python)) { throw 'Cree .venv e instale requirements-dev.txt primero.' }
& $python (Join-Path $PSScriptRoot 'descargar_herramientas.py')
$kind = Join-Path $tools 'kind.exe'
& $kind version
& (Join-Path $tools 'helm.exe') version --short

