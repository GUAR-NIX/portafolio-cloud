# Guía paso a paso de la API de clima en Kubernetes

## Preparar los datos y probar la API

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
python descargar_datos.py
$env:PYTHONPATH='src'
python scripts\generar_reporte.py
pytest -q
uvicorn clima_api.principal:app --app-dir src --reload
```

Abra `http://localhost:8000/docs`. El Parquet contiene 2024 completo para doce ciudades y queda dentro de la
imagen: esta decisión hace que la demostración sea autocontenida, pero no sustituye una base de datos para un
servicio con actualizaciones continuas.

## Desplegar en kind

Con Docker Desktop abierto (probado con 16 GB asignados; el clúster con monitoreo usa ~2,5 GB):

```powershell
.\scripts\descargar_herramientas.ps1   # kind v0.30.0 y Helm v3.19.0 en tools\
.\scripts\desplegar_kind.ps1           # imágenes, clúster, metrics-server v0.8.1, ingress-nginx v1.15.1 y chart
.\scripts\instalar_monitoreo.ps1       # kube-prometheus-stack 92.13.1 reducido + dashboard + ServiceMonitor
```

- Streamlit: `http://localhost:8080` (NodePort).
- API por Ingress: `curl.exe -H "Host: clima.local" http://localhost:8081/api/clima?ciudad=Quito&fecha=2024-06-01`
  (el Ingress reescribe `/api/<ruta>` a `/<ruta>`).
- Grafana (lectura anónima): `kubectl port-forward -n monitoreo service/monitoreo-grafana 3000:80` y abrir
  `http://localhost:3000/d/clima-ecuador`.

Los scripts usan `$ErrorActionPreference = 'Continue'` y revisan `$LASTEXITCODE`: Windows PowerShell 5.1 convierte
en error terminante el progreso que docker, kind y helm escriben en stderr. Por la misma versión, los parches JSON
de `kubectl patch` van en archivos (`kind/parche-*.json`).

## Medir

```powershell
.\scripts\prueba_carga.ps1        # Job de Locust dentro del clúster: 1 fija, 4 fijas, HPA vía ClusterIP y vía Ingress
.\scripts\demostrar_rolling.ps1   # helm upgrade 1.0.0→1.0.1 y helm rollback bajo carga, contando errores
python scripts\comparar_p95.py    # p95 de Prometheus frente a Locust y tabla en régimen (port-forward a 9090)
.\scripts\capturar_grafana.ps1    # captura del dashboard con Edge sin interfaz
```

La carga nunca pasa por `kubectl port-forward`: el túnel sería el cuello de botella y no probaría la red del
clúster. Los resultados quedan en `reports/` y las figuras en `reports/figures/`.

Al finalizar, libere todos los recursos:

```powershell
.\scripts\eliminar_cluster.ps1
```

## Preguntas de entrevista

### 1. ¿Qué diferencia hay entre liveness y readiness?

Liveness decide si kubelet debe reiniciar el contenedor; readiness decide si el pod puede recibir tráfico. Una
dependencia temporal no debería provocar un bucle de reinicios.

### 2. ¿Por qué definir requests y limits?

El scheduler usa requests para ubicar pods y el HPA calcula utilización contra el CPU solicitado. Los limits
protegen al nodo, aunque uno de CPU demasiado bajo puede causar throttling.

### 3. ¿Cómo decide escalar el HPA?

Compara la utilización media observada con el objetivo y ajusta réplicas dentro del mínimo y máximo. Necesita
metrics-server y requests de CPU configurados.

### 4. ¿Por qué maxUnavailable es cero?

Durante la actualización se crea un pod nuevo antes de retirar el anterior. Con readiness correcta mantiene al
menos una réplica disponible, a costa de capacidad temporal adicional.

### 5. ¿Qué mide Prometheus?

La API expone contadores e histogramas HTTP. ServiceMonitor enseña a Prometheus dónde raspar `/metricas`; Grafana
consulta series para req/s, p95, errores y réplicas.

### 6. ¿Qué prueba kind que no prueban los tests unitarios?

Valida imágenes, manifiestos, DNS de servicios, sondas, permisos y comportamiento del scheduler en un Kubernetes
real. No reproduce la red ni los servicios administrados de una nube.

### 7. ¿Por qué una imagen por servicio?

API y app tienen ciclos y dependencias distintas. Separarlas reduce superficie, permite escalarlas de forma
independiente y evita instalar Streamlit en la API.

### 8. ¿Cómo funciona un rollback de Helm?

Helm conserva revisiones de valores y manifiestos. `helm rollback` aplica una revisión anterior; Kubernetes hace
el despliegue gradual y las sondas impiden enviar tráfico al pod no preparado.

### 9. ¿Cómo migraría a EKS, GKE o AKS?

Publicaría imágenes en el registro administrado, reemplazaría NodePort por Ingress/LoadBalancer, usaría identidad
de carga, secretos administrados, almacenamiento externo, TLS, autoscaling de nodos y observabilidad gestionada.

### 10. ¿Qué limita esta prueba de carga?

Kind comparte CPU y disco con Docker Desktop; mide la solución local, no la capacidad de producción. Para una
conclusión de capacidad se repite en infraestructura controlada, con calentamiento, percentiles y varias corridas.

### 11. ¿Por qué el dashboard está versionado?

Hace revisable y reproducible la observabilidad; evita depender de cambios manuales en Grafana y permite promover
el mismo panel entre ambientes.

### 12. El HPA subió a 4 réplicas, pero el rendimiento casi no mejoró. ¿Por qué?

Un Service ClusterIP reparte por conexión TCP (kube-proxy), no por petición. Los clientes con keep-alive abrieron
sus conexiones cuando solo había un pod y siguieron usándolo: en la medición, un pod quedó al 100 % de su limit y
los otros tres casi ociosos. El HPA veía un promedio por encima del objetivo y mantenía 4 réplicas que no ayudaban.
Con ingress-nginx, que reparte cada petición entre los Endpoints, los cuatro pods trabajaron. Otras salidas son
un service mesh, un balanceador L7 o clientes que renueven conexiones.

### 13. ¿Por qué un rolling update con maxUnavailable 0 y readiness todavía puede dar errores?

Porque retirar el pod de los Endpoints solo frena conexiones nuevas. Las conexiones keep-alive ya abiertas siguen
llegando al pod viejo y, cuando Uvicorn recibe SIGTERM, las cierra: el cliente que justo enviaba recibe
`Connection reset`. La corrección fue un preStop que llama a `/drenar` (solo desde 127.0.0.1): desde ese momento
cada respuesta lleva `Connection: close` y `/listo` devuelve 503, así que los clientes se mudan a pods vivos antes
del SIGTERM. Se mide con carga continua y conteo de errores, no se supone.

## Seguridad y límites

Los contenedores corren sin root y sin capacidades Linux. La API usa filesystem de solo lectura. El proyecto no
crea clústeres ni registros en la nube y no requiere credenciales.

