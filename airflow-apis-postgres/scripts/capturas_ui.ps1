# Capturas de la UI de Airflow (http://localhost:8080) con Chrome sin ventana -> reports\capturas\*.png
# Uso: .\scripts\capturas_ui.ps1      (con el stack levantado)
$chrome = "C:\Program Files\Google\Chrome\Application\chrome.exe"
if (-not (Test-Path $chrome)) { $chrome = "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe" }
$destino = Join-Path $PSScriptRoot "..\reports\capturas" | Resolve-Path
$perfil = Join-Path $env:TEMP "chrome_capturas_airflow"

$paginas = [ordered]@{
    "01_dags"            = "http://localhost:8080/dags"
    "02_grid_clima"      = "http://localhost:8080/dags/ingesta_clima_ecuador"
    "03_grid_banco"      = "http://localhost:8080/dags/ingesta_banco_mundial"
    "04_marts"           = "http://localhost:8080/dags/construir_marts"
    "05_assets"          = "http://localhost:8080/assets"
}
foreach ($p in $paginas.GetEnumerator()) {
    $archivo = Join-Path $destino "$($p.Key).png"
    $argumentos = @("--headless=new", "--disable-gpu", "--hide-scrollbars", "--lang=es", "--user-data-dir=`"$perfil`"",
                    "--window-size=1600,900", "--virtual-time-budget=20000", "--screenshot=`"$archivo`"", $p.Value)
    Start-Process -FilePath $chrome -ArgumentList $argumentos -Wait -NoNewWindow -RedirectStandardError "$perfil.log"
    Write-Output "$($p.Key).png"
}
