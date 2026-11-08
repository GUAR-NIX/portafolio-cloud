# Guía paso a paso (Windows + PowerShell)

Todo se hace desde la raíz del proyecto (la carpeta `airflow-apis-postgres`). No hace falta cuenta en ninguna nube ni
clave de API: Open-Meteo y el Banco Mundial son públicos y gratuitos.

| Fase | Qué haces | Tiempo |
|---|---|---|
| 0 | Requisitos y `.env` | 5 min |
| 1 | Levantar Airflow + Postgres | 3–5 min (la primera vez descarga ~700 MB de imagen) |
| 2 | Pruebas (`pytest`) dentro del contenedor | 1 min |
| 3 | Activar los DAG: backfill de 2000–2025 (Banco Mundial) y oct-2025 → ayer (clima) | ≈ 1,3 h |
| 4 | Revisar la UI, los logs y la auditoría | 10 min |
| 5 | Exportar cifras y gráficos; conectar Power BI | 5 min |
| 6 | Demostrar un control de calidad que falla | 5 min |
| 7 | Apagar | 1 min |

---

## 0. Requisitos

- **Docker Desktop** abierto (*Engine running*). Con 16 GB asignados a WSL sobra: el compose limita la memoria
  de cada servicio y suma ≈ 5,3 GB como máximo.
- Python 3.12+ solo para los scripts del equipo (reporte y gráficos). Airflow **no** se instala en Windows.

```powershell
Copy-Item .env.example .env
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
```

## 1. Levantar Airflow

```powershell
docker compose up -d --build
docker compose ps          # postgres, airflow-apiserver, airflow-scheduler, airflow-dag-processor: healthy
```

`airflow-init` corre una vez: migra la base de metadatos y crea los **pools** `api_open_meteo` (1 cupo) y
`api_banco_mundial` (3 cupos). Luego abre <http://localhost:8080>. En local no hay pantalla de inicio de
sesión (`SimpleAuthManager` con todos administradores): **solo para tu PC**.

| Servicio | Memoria máx. | Qué hace |
|---|---:|---|
| `postgres` | 768 MB | metadatos de Airflow (base `airflow`) + almacén (`datos`) + pruebas (`datos_pruebas`) |
| `airflow-apiserver` | 1,5 GB | UI y API REST; también la *Execution API* que usan las tareas en Airflow 3 |
| `airflow-scheduler` | 2 GB | planifica y, con LocalExecutor, **ejecuta** las tareas (8 en paralelo) |
| `airflow-dag-processor` | 768 MB | parsea los DAG cada 30 s (en Airflow 3 es un proceso aparte) |

## 2. Pruebas

```powershell
docker compose run --rm pruebas                      # 55 pruebas: DAG + lógica + SQL contra Postgres real
.\.venv\Scripts\python -m pytest -q                  # en Windows: solo las 31 puras (las demás se omiten)
```

- `test_dags.py`: los DAG cargan sin errores de importación, con pools que existen, se serializan como lo hace
  el dag-processor, tienen reintentos, timeouts, callbacks y la forma esperada.
- `test_transformar.py`: parseo del JSON (orden de ciudades, día faltante, variable faltante), elección de API,
  reintentos HTTP (429 con `Retry-After`, 5xx, timeouts, 400 sin reintento) y reglas de calidad.
- `test_sql.py`: raw → staging → controles → marts en una base vacía, idempotencia, 5 corrupciones que
  **deben** hacer fallar los controles y el *rollback* de los marts.

## 3. Backfill

Los DAG nacen pausados (`DAGS_ARE_PAUSED_AT_CREATION`). Al activarlos, `catchup=True` crea una ejecución por
cada intervalo pendiente:

```powershell
docker compose exec airflow-scheduler airflow dags unpause construir_marts
docker compose exec airflow-scheduler airflow dags unpause ingesta_banco_mundial   # 26 ejecuciones (2000–2025)
docker compose exec airflow-scheduler airflow dags unpause ingesta_clima_ecuador   # una por día desde 1-oct-2025
```

(También se puede con el interruptor de cada DAG en la UI.) Para un rango concreto sin depender de `catchup`,
Airflow 3 tiene backfills gestionados por el scheduler, visibles en la UI:

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id ingesta_clima_ecuador `
    --from-date 2025-10-01 --to-date 2025-10-31 --reprocess-behavior failed
```

**Por qué tarda ~1,3 h el clima:** cada ciudad cuenta como una llamada de la cuota gratuita de Open-Meteo
(5.000 por hora, 10.000 por día). 383 días × 13 ciudades = 4.979 llamadas: el pool de 1 cupo y la pausa de 10 s
entre peticiones mantienen el ritmo en ≤ 4.680 llamadas/hora. Re-ejecutar un día que ya está en raw con datos
definitivos **no** vuelve a llamar a la API (salvo con el parámetro `forzar_descarga`).

Seguimiento desde la terminal:

```powershell
docker compose exec postgres psql -U datos -d datos -c "SELECT count(DISTINCT fecha) FROM staging.clima_diario"
docker compose exec airflow-scheduler airflow dags list-runs ingesta_clima_ecuador --state failed
```

## 4. UI, logs y auditoría

- **DAGs** → los tres en verde; **Assets** → el grafo `staging/clima_diario` + `staging/indicadores_bm` →
  `construir_marts`; **Browse → Task Instances** para filtrar por estado.
- Logs de cada intento en `logs\dag_id=...\run_id=...\task_id=...\attempt=N.log` (JSON por línea). Una muestra
  queda en `reports\logs\`.
- En la base `datos`: `auditoria.cargas` (filas y segundos por tarea y partición) y
  `auditoria.controles_calidad` (cada control de cada ejecución, con valor y rango).

```sql
-- ¿qué controles han fallado alguna vez?
SELECT capa, control, count(*), min(registrado_en) FROM auditoria.controles_calidad
WHERE NOT ok GROUP BY 1, 2;
```

## 5. Cifras, gráficos y BI

```powershell
.\.venv\Scripts\python scripts\exportar_resultados.py     # reports\resultados.json + reports\figures\*.png
```

**Power BI / DBeaver:** servidor `localhost:5433`, base `datos`, usuario `datos` / clave `datos` (solo local).
Tablas para el modelo: `marts.fct_clima_diario` (hechos) ↔ `marts.dim_ciudad` (por `ciudad`), y
`marts.clima_mensual_ciudad`, `marts.indicadores_pais_anual`, `marts.ecuador_vs_region` como agregados.

## 6. Ver un control de calidad fallar (y recuperarse)

```powershell
# 1) corromper un día ya cargado
docker compose exec postgres psql -U datos -d datos -c "UPDATE staging.clima_diario SET temp_max_c = 80 WHERE fecha = '2026-01-15' AND ciudad = 'Loja'"
# 2) re-ejecutar solo los controles de ese día -> FALLA (rojo en la UI, sin reintentos)
docker compose exec airflow-scheduler airflow tasks clear ingesta_clima_ecuador -t controles_calidad -s 2026-01-15T11:00:00+00:00 -e 2026-01-15T11:00:00+00:00 -y
# 3) re-ejecutar el día completo -> staging se reconstruye desde raw y vuelve a verde
docker compose exec airflow-scheduler airflow tasks clear ingesta_clima_ecuador -s 2026-01-15T11:00:00+00:00 -e 2026-01-15T11:00:00+00:00 -y
```

(Las fechas de `-s/-e` son el `logical_date` en UTC: el día 15 empieza a las 06:00 de Ecuador = 11:00 UTC.)

## Problemas comunes

| Síntoma | Causa y solución |
|---|---|
| Tras apagar el PC, algunas ejecuciones de clima quedan *failed* con todas sus tareas en *skipped* | Estaban a medias y superaron `dagrun_timeout` (1 h) al volver. Son idempotentes: `airflow tasks clear ingesta_clima_ecuador -s <inicio> -e <fin> -y` |
| `extraer_a_raw` en *up_for_retry* con HTTP 429 | Se agotó la cuota horaria de Open-Meteo (p. ej., otra herramienta usando la misma IP). Los reintentos exponenciales lo resuelven solos; si no, espera una hora |
| Tareas en *queued* que no avanzan | Falta el pool: `docker compose run --rm airflow-init` lo vuelve a crear |
| Cada tarea tarda > 10 s en arrancar | Revisa que los assets sigan siendo por nombre (`Asset(name=...)`), no por URI: ver el README |
| El puerto 5433 u 8080 ya está ocupado | Cambia el lado izquierdo del mapeo en `docker-compose.yml` (`"5434:5432"`) |

## 7. Apagar

```powershell
docker compose down          # conserva los datos en el volumen postgres-datos
docker compose down -v       # borra también la base (el próximo `up` empieza de cero)
```

---

## Llevarlo a la nube (sin desplegarlo)

El código no cambia: los DAG solo dependen de la conexión `datos_postgres` y de los pools. Lo que cambia es
quién opera Airflow y dónde vive la base.

| Pieza local | AWS · **MWAA** | GCP · **Cloud Composer 3** |
|---|---|---|
| `dags/` + `include/` + `sql/` | Bucket S3 `s3://<bucket>/dags/` (MWAA sincroniza solo); `include/` y `sql/` dentro de `dags/` o como *plugins.zip* | Bucket del entorno `gs://<bucket>/dags/` (`gcloud composer environments storage dags import`) |
| `Dockerfile` (pytest) | `requirements.txt` del entorno, con el archivo de restricciones de la misma versión | Paquetes PyPI del entorno |
| Postgres `datos` | **RDS for PostgreSQL** (o Redshift si crece); subred privada | **Cloud SQL for PostgreSQL** (o BigQuery) con IP privada |
| `AIRFLOW_CONN_DATOS_POSTGRES` | **Secrets Manager** como *secrets backend* (`airflow/connections/datos_postgres`) | **Secret Manager** como *secrets backend* |
| Pools (`airflow-init`) | `airflow pools set` por la CLI de MWAA o un DAG de arranque | `gcloud composer environments run ... pools set` |
| Logs en `./logs` | CloudWatch Logs (por tarea) | Cloud Logging |
| `alertar_fallo` | SNS → correo / Slack | Pub/Sub o Cloud Monitoring → correo / Slack |
| LocalExecutor | Celery gestionado (workers con autoescalado `min/max_workers`) | Workers en GKE Autopilot con autoescalado |
| `SimpleAuthManager` (sin login) | IAM: `airflow:CreateWebLoginToken` por rol | IAM de Google (roles `composer.user`) |
| Red | VPC privada; salida a Internet por NAT Gateway (las APIs son públicas) | VPC con Cloud NAT |

Pasos que seguiría (en un repositorio con Terraform, como en el proyecto CLOUD):

1. Fijar la versión: MWAA y Composer publican qué versiones de Airflow soportan; los DAG usan solo
   `airflow.sdk` (Airflow 3), así que la versión mínima es 3.0.
2. Crear la base (RDS / Cloud SQL), el secreto con la URI y el entorno (MWAA `mw1.small` / Composer *small*).
3. CI/CD: el mismo workflow de GitHub Actions corre las pruebas y, si pasan en `main`, hace
   `aws s3 sync dags/ s3://<bucket>/dags/` o `gcloud storage rsync`. El *DAG integrity test* evita subir un DAG
   roto que dejaría de verse en la UI.
4. Costo aproximado (precio de lista, oct-2026, sin capa gratuita): un MWAA `mw1.small` ronda **USD 358 al
   mes** encendido 24/7 (≈ 405 con base de datos, NAT y logs; supuestos en el README, sección «Costo estimado en la nube») y Composer 3 *small* un orden similar; por eso para este volumen (≈ 5 MB/año) también
   sería razonable **EventBridge Scheduler + Lambda** o **Cloud Scheduler + Cloud Run jobs**. Airflow se justifica
   cuando hay decenas de DAG con dependencias, backfills y un equipo que los opera.

---

## Preguntas de entrevista (con respuesta)

**1. ¿Qué significa que un DAG sea idempotente y cómo lo lograste?**
Que correr la misma partición una o diez veces deja el mismo resultado. Cada ejecución procesa *su* intervalo
(`data_interval_start`), nunca "hoy": raw hace *upsert* por fecha, staging borra e inserta la fecha en una
transacción y los marts se reconstruyen completos en otra. La prueba `test_recargar_la_misma_fecha_es_idempotente`
lo verifica.

**2. ¿Qué diferencia hay entre `logical_date`, `data_interval_start` y la fecha en que corre?**
Con `CronDataIntervalTimetable("0 6 * * *")` la ejecución que cubre el 15 de enero tiene intervalo
[15-ene 06:00, 16-ene 06:00) y corre al cerrarse el intervalo, el 16 a las 06:00. En Airflow 3 el horario por
defecto es `CronTriggerTimetable` (sin intervalo); lo elegí explícito para que "un run = un día" sea claro.

**3. ¿Cómo hiciste el backfill?**
`catchup=True` + `start_date` en el pasado: al activar el DAG, el scheduler crea todas las ejecuciones
pendientes, limitadas por `max_active_runs`. Para un rango puntual, `airflow backfill create`.

**4. Tu API tiene cuota. ¿Cómo evitas que el backfill la agote?**
Un pool de 1 cupo serializa las peticiones; antes de pedir, la tarea espera lo que falte para 10 s desde la
última descarga (raw es el reloj compartido); un día con dato definitivo no se vuelve a pedir; y el cliente
respeta `Retry-After` en los 429. Calculé el ritmo: 13 ciudades × 6 peticiones/min × 60 = 4.680 llamadas/h, bajo
el límite de 5.000.

**5. ¿Dónde pones los reintentos?**
En dos niveles. El cliente HTTP reintenta errores transitorios (timeouts, 429, 5xx) con espera exponencial; un
400 falla de inmediato porque reintentarlo no sirve. Airflow reintenta la tarea completa 3 veces con espera
exponencial (30 s → 10 min). Los controles de calidad tienen `retries=0`: un dato malo no mejora reintentando.

**6. ¿Por qué SQL propio y no Great Expectations o Soda?**
Son ~25 reglas sobre tablas pequeñas en la misma base. GX añade cientos de MB de dependencias que chocan con las
restricciones de versiones de Airflow; Soda necesita otra configuración. Una lista de `Control(nombre, sql,
mínimo, máximo, crítico)` es legible, se prueba con pytest y guarda cada resultado en una tabla para un tablero
de calidad. Si el equipo crece, se traduce 1:1 a checks de Soda.

**7. Cuéntame un bug que encontraste.**
Construía los marts en una tarea y los validaba en otra. Durante el backfill la conciliación
"filas del mart = filas de staging" fallaba (diferencia de −13: un día de 13 ciudades) porque otras ejecuciones
seguían escribiendo en staging entre las dos tareas. Lo resolví con *write-audit-publish* en una sola
transacción `REPEATABLE READ`: construir y validar ven la misma foto, y solo se hace `COMMIT` si pasa.

**8. ¿Cómo se comunican los DAG entre sí?**
Con *assets* (antes *datasets*): la tarea de calidad declara `outlets=[ASSET_CLIMA]` y `construir_marts` tiene
`schedule=(ASSET_CLIMA | ASSET_BANCO_MUNDIAL)`. Es mejor que un `ExternalTaskSensor` porque no hay que alinear
horarios ni ocupar un cupo esperando, y varios eventos seguidos se agrupan en una sola ejecución.

**9. ¿Para qué sirve el sensor en modo `reschedule`?**
`esperar_api` comprueba que la API del Banco Mundial responde; en modo `reschedule` libera el cupo entre
intentos (en `poke` lo ocuparía todo el tiempo). Para esperas largas en producción usaría un sensor *deferrable*
con el *triggerer*.

**10. ¿Qué es el mapeo dinámico y por qué lo usaste?**
`extraer_indicador.expand(indicador=[...])` crea una tarea por indicador en tiempo de ejecución. Si falla uno,
solo ese se reintenta, y cada uno aparece con su log en la UI.

**11. ¿Por qué guardar el JSON crudo en vez de cargar directo a staging?**
Para poder reconstruir staging sin volver a llamar a la API (cuota, la API pudo cambiar) y para auditar: cada
respuesta guarda URL, parámetros, SHA-256 y número de descargas. Staging se arma leyendo raw, nunca desde XCom.

**12. ¿Qué no pondrías en el código del DAG a nivel de módulo?**
Nada que haga red o consultas: el dag-processor parsea los archivos cada 30 s. Toda la lógica vive en
`include/ingesta` (sin Airflow), y los DAG solo la orquestan. La prueba de integridad de los DAG mide eso.

**13. ¿Por qué LocalExecutor y no Celery?**
Una sola máquina y ~1.500 tareas: Celery agrega Redis y workers sin beneficio. En la nube el ejecutor lo
gestiona MWAA/Composer.

**14. ¿Cómo lo llevarías a producción?**
Ver la sección anterior: MWAA o Composer, la base en RDS/Cloud SQL, la conexión en un *secrets backend*, CI que
prueba y sincroniza `dags/`, alertas por SNS/Slack y un rol de solo lectura para BI.
