# Rolling update 1.0.0→1.0.1 y helm rollback bajo carga continua, contando errores.
$ErrorActionPreference = 'Continue'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..'))
$kind = '.\tools\kind.exe'
docker build -f docker/api.Dockerfile -t clima-api:1.0.1 . 2>&1 | Select-Object -Last 2
& $kind load docker-image clima-api:1.0.1 --name clima-ecuador 2>&1 | Select-Object -Last 1
.\.venv\Scripts\python.exe scripts\carga_cluster.py rolling @args
if ($LASTEXITCODE -ne 0) { throw 'Falló la demostración de rolling update' }
.\.venv\Scripts\python.exe scripts\graficar_cluster.py
