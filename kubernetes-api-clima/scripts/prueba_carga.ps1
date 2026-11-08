# Carga dentro del clúster (Job de Locust): 1 réplica fija, 4 fijas y HPA 1→4→1.
# Requiere desplegar_kind.ps1 (y opcionalmente instalar_monitoreo.ps1) ejecutados antes.
# 'Continue': PowerShell 5.1 trata el stderr de helm/kubectl como error; se valida $LASTEXITCODE.
$ErrorActionPreference = 'Continue'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..'))
.\.venv\Scripts\python.exe scripts\carga_cluster.py escenarios @args
if ($LASTEXITCODE -ne 0) { throw 'Falló la prueba de carga' }
.\.venv\Scripts\python.exe scripts\graficar_cluster.py
