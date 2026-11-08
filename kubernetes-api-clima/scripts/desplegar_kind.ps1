param([string]$Version = '1.0.0')
# 'Continue' porque PowerShell 5.1 trata el stderr de docker/kind/kubectl como error; se valida $LASTEXITCODE.
$ErrorActionPreference = 'Continue'
$raiz = Resolve-Path (Join-Path $PSScriptRoot '..')
Set-Location $raiz
$kind = Join-Path $raiz 'tools\kind.exe'
$helm = Join-Path $raiz 'tools\helm.exe'
if (-not (Test-Path $kind) -or -not (Test-Path $helm)) { throw 'Ejecute scripts\descargar_herramientas.ps1 primero.' }

# Versiones fijadas: un `releases/latest` haría que el mismo script instale cosas distintas cada mes.
$versionMetricsServer = 'v0.8.1'
$versionIngressNginx = 'controller-v1.15.1'

function Invocar([scriptblock]$bloque) {
  # En PowerShell 5.1 el progreso que docker/kind escriben en stderr se volvería error terminante;
  # el éxito se decide por el código de salida.
  $anterior = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  & $bloque 2>&1 | ForEach-Object { "$_" }
  $codigo = $LASTEXITCODE
  $ErrorActionPreference = $anterior
  if ($codigo -ne 0) { throw "Falló ($codigo): $bloque" }
}

Invocar { docker build -f docker/api.Dockerfile -t "clima-api:$Version" . }
Invocar { docker build -f docker/app.Dockerfile -t "clima-app:$Version" . }
$clusters = & $kind get clusters 2>&1 | ForEach-Object { "$_" }
if ($clusters -notcontains 'clima-ecuador') { Invocar { & $kind create cluster --config kind/config.yaml --wait 120s } }
Invocar { & $kind load docker-image "clima-api:$Version" "clima-app:$Version" --name clima-ecuador }

Invocar { kubectl apply -f "https://github.com/kubernetes-sigs/metrics-server/releases/download/$versionMetricsServer/components.yaml" }
# kind usa certificados autofirmados en kubelet; solo aceptable en un clúster local.
$args_ms = kubectl get deployment metrics-server -n kube-system -o jsonpath='{.spec.template.spec.containers[0].args}' 2>&1 | ForEach-Object { "$_" }
if ($args_ms -notmatch 'kubelet-insecure-tls') {
  # Parche en archivo: PowerShell 5.1 quita las comillas internas de un JSON pasado como argumento.
  Invocar { kubectl patch deployment metrics-server -n kube-system --type json --patch-file kind/parche-metrics-server.json }
}
Invocar { kubectl rollout status deployment/metrics-server -n kube-system --timeout=180s }

Invocar { kubectl apply -f "https://raw.githubusercontent.com/kubernetes/ingress-nginx/$versionIngressNginx/deploy/static/provider/kind/deploy.yaml" }
# El manifiesto v1.15 ya no fija nodeSelector: sin este parche el controlador puede caer en el worker,
# que no tiene el puerto 80 publicado hacia Windows.
Invocar { kubectl patch deployment ingress-nginx-controller -n ingress-nginx --patch-file kind/parche-ingress-nginx.json }
Invocar { kubectl rollout status deployment/ingress-nginx-controller -n ingress-nginx --timeout=180s }

Invocar { & $helm upgrade --install clima-ecuador charts/clima-ecuador --set ingress.enabled=true `
  --set api.image.tag=$Version --set app.image.tag=$Version --wait --timeout 5m }
kubectl get nodes,pods,hpa,service,ingress -o wide

# Verificación: Streamlit por NodePort (8080) y API a través de ingress-nginx (8081).
# curl.exe porque Invoke-RestMethod de PowerShell 5.1 no permite cambiar la cabecera Host.
$limite = (Get-Date).AddSeconds(90)
do {
  $app = curl.exe -s http://localhost:8080/_stcore/health
  $api = curl.exe -s -H 'Host: clima.local' http://localhost:8081/api/salud
  if ($app -eq 'ok' -and $api -match 'vivo') { break }
  Start-Sleep -Seconds 3
} while ((Get-Date) -lt $limite)
if ($api -notmatch 'vivo') { throw "La API no respondió por el Ingress: $api" }
"Streamlit: $app · API por Ingress: $api"
