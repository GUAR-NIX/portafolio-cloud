# Capturas de la UI de Dagster (http://localhost:3000) con Chrome sin ventana -> reports\capturas\*.png
# Uso: .\scripts\capturas_ui.ps1 [nombre=ruta ...]     (con el stack levantado)
param([string[]]$Paginas)
$chrome = "C:\Program Files\Google\Chrome\Application\chrome.exe"
if (-not (Test-Path $chrome)) { $chrome = "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe" }
$destino = Join-Path $PSScriptRoot "..\reports\capturas" | Resolve-Path
$perfil = Join-Path $env:TEMP "chrome_capturas_dagster"

$porDefecto = [ordered]@{
    "01_linaje_activos"   = "/asset-groups"
    "02_particiones_anio" = "/assets/ghcn/ghcn_crudo_parquet?view=partitions"
    "03_backfill"         = "/overview/backfills"
    "04_checks_mart"      = "/assets/marts/mart_clima_mensual_pais?view=checks"
    "05_corridas"         = "/runs"
}
$lista = [ordered]@{}
if ($Paginas) { foreach ($p in $Paginas) { $k, $v = $p -split "=", 2; $lista[$k] = $v } } else { $lista = $porDefecto }

foreach ($p in $lista.GetEnumerator()) {
    $archivo = Join-Path $destino "$($p.Key).png"
    $argumentos = @("--headless=new", "--disable-gpu", "--hide-scrollbars", "--lang=es", "--user-data-dir=`"$perfil`"",
                    "--window-size=1600,950", "--virtual-time-budget=25000", "--screenshot=`"$archivo`"",
                    "http://localhost:3000$($p.Value)")
    Start-Process -FilePath $chrome -ArgumentList $argumentos -Wait -NoNewWindow -RedirectStandardError "$perfil.log"
    Write-Output "$($p.Key).png"
}
