# Captura el dashboard de Grafana con Edge sin interfaz (requiere el port-forward a localhost:3000).
param([string]$Salida = 'reports\figures\06_grafana.png', [int]$Minutos = 15)
$ErrorActionPreference = 'Stop'
$edge = 'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe'
$perfil = Join-Path $env:TEMP 'edge-grafana-clima'
$destino = Join-Path (Resolve-Path (Join-Path $PSScriptRoot '..')) $Salida
$url = "http://localhost:3000/d/clima-ecuador/api-clima-ecuador?orgId=1&from=now-$($Minutos)m&to=now&kiosk"
# Start-Process -Wait: msedge.exe devuelve el control antes de escribir la imagen.
Start-Process -FilePath $edge -Wait -WindowStyle Hidden -ArgumentList @(
  '--headless=new', '--disable-gpu', '--hide-scrollbars', "--user-data-dir=$perfil",
  '--window-size=1600,990', '--virtual-time-budget=15000', "--screenshot=$destino", "`"$url`""
)
Get-Item $destino | Select-Object Name, Length
