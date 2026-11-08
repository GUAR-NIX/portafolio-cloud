# 'Continue': PowerShell 5.1 trata el stderr de helm/kubectl como error; se valida $LASTEXITCODE.
$ErrorActionPreference = 'Continue'
$raiz = Resolve-Path (Join-Path $PSScriptRoot '..')
Set-Location $raiz
$helm = Join-Path $raiz 'tools\helm.exe'
$versionStack = '92.13.1'

& $helm repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update
& $helm repo update prometheus-community
& $helm upgrade --install monitoreo prometheus-community/kube-prometheus-stack --version $versionStack `
  --namespace monitoreo --create-namespace -f monitoreo/valores-kube-prometheus.yaml --wait --timeout 10m
if ($LASTEXITCODE -ne 0) { throw 'Falló la instalación de kube-prometheus-stack' }

# El sidecar de Grafana carga cualquier ConfigMap con la etiqueta grafana_dashboard=1.
kubectl create configmap dashboard-clima --from-file=monitoreo/dashboard-clima.json `
  -n monitoreo --dry-run=client -o yaml | kubectl apply -f -
kubectl label configmap dashboard-clima -n monitoreo grafana_dashboard=1 --overwrite

& $helm upgrade clima-ecuador charts/clima-ecuador --reuse-values --set serviceMonitor.enabled=true --wait
if ($LASTEXITCODE -ne 0) { throw 'Falló la activación del ServiceMonitor' }
kubectl get pods -n monitoreo
'Grafana: kubectl port-forward -n monitoreo service/monitoreo-grafana 3000:80  ->  http://localhost:3000/d/clima-ecuador'
