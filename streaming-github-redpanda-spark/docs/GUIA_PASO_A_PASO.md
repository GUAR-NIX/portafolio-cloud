# Guía paso a paso (Windows + PowerShell)

Cómo levantar la plataforma, reproducir las corridas medidas del README, demostrar la recuperación, consultar oro,
abrir el tablero, apagarlo todo, cómo se llevaría a la nube y preguntas de entrevista con respuesta.

Todo corre en Docker. El Python del equipo (3.14) solo se usa para dos scripts que no necesitan paquetes:
`scripts/descargar_datos.py` y `scripts/orquestar.py`.

## 0. Requisitos

| Qué | Para qué |
|---|---|
| Docker Desktop con ~6 GB de memoria libres | Redpanda, MinIO, Spark, productor y tablero (límites en `docker-compose.yml`) |
| ~10 GB de disco | 6,2 GB de GH Archive crudo + 0,6 GB preparado + volúmenes de Redpanda y MinIO |
| Python 3.x en el anfitrión | descarga y orquestación (solo biblioteca estándar) |

## 1. Datos

```powershell
python scripts\descargar_datos.py                 # 72 archivos horarios (16–18 sep 2025), 6,2 GB, reanudable
docker compose build                              # imagen streaming-gh:local (Spark) y minio-local (MinIO compilado)
docker compose run --rm productor python -m streaming_gh.preparar --procesos 4
```

`preparar` reduce cada evento a los campos que usa el negocio (4,2 KB → ~0,4 KB) y deja una hora por archivo,
ordenada por `created_at`, en `data/interim/eventos/`. Tarda ~5 min con 4 procesos.

> **¿Por qué septiembre de 2025?** Desde el 7-oct-2025 la API de eventos de GitHub recorta los payloads (sin
> commits ni el objeto `pull_request`), y con eso se pierde el lenguaje del repositorio. Un archivo horario de
> noviembre de 2025 pesa 37 MB frente a 95 MB en septiembre.

## 2. Levantar la infraestructura

```powershell
docker compose up -d redpanda redpanda-console minio minio-init
docker compose ps
```

| Servicio | URL | Para qué |
|---|---|---|
| Redpanda Console | http://localhost:8080 | temas, particiones, mensajes y offsets |
| Consola de MinIO | http://localhost:9001 (`minioadmin` / `minioadmin`) | ver las tablas Delta en el bucket `lakehouse` |
| Spark UI | http://localhost:4040 (con el pipeline corriendo) | pestaña *Structured Streaming*: tasas y duraciones |
| Tablero | http://localhost:8501 | capa oro |

## 3. Pruebas

```powershell
docker compose run --rm pruebas                       # 27 pruebas, ~2 min (Spark local dentro del contenedor)
docker compose run --rm pruebas ruff check src tests scripts dashboard
```

Las pruebas usan `data/muestra/gharchive_muestra.json.gz` (1.514 eventos crudos reales, 15 tipos), así que corren
igual en la CI sin descargar nada. Cubren: reducción en el origen, el productor con reloj simulado (ritmo,
inyección reproducible, tardíos), bronce/plata/oro en lote, **watermark en streaming de verdad** (un tardío se
descarta y otro entra), deduplicación entre micro-lotes, escritura idempotente y el pipeline plata → oro completo.

## 4. Una corrida a mano

```powershell
$env:CORRIDA = "demo"
docker compose up -d spark                                            # 5 consultas: bronce, plata, 3 de oro
docker compose run --rm productor python -m streaming_gh.productor --corrida demo `
    --desde 2025-09-16T14:00:00Z --hasta 2025-09-16T16:00:00Z --factor 60
docker compose logs -f spark                                          # Ctrl+C para salir del log
docker compose run --rm tareas python -m streaming_gh.verificar --corrida demo
docker compose stop spark
```

Cada corrida aísla su tema (`gh-eventos-<corrida>`), sus tablas (`s3a://lakehouse/<corrida>/...`), sus
checkpoints y sus métricas (`reports/metricas/<corrida>/`).

## 5. Reproducir las corridas del README

`scripts/orquestar.py` hace todo el ciclo (pipeline → productor → esperar a que bronce/plata alcancen y oro se
estabilice → detener → verificar) y es reanudable: si el equipo se apaga, vuelve a ejecutar el mismo comando.

```powershell
python scripts\orquestar.py corrida --nombre principal --factor 120        # 3 días de GitHub en ~40 min
python scripts\orquestar.py recuperacion --repetir                           # 18 h a 240x con 3 docker kill
python scripts\orquestar.py falla                                           # muerte justo tras escribir un lote
python scripts\orquestar.py benchmark --factores 60,600,0                    # rendimiento a 3 velocidades
docker compose run --rm tareas python -m streaming_gh.consultas_oro --corrida principal   # reports/consultas/*.csv
docker compose run --rm tareas python -m streaming_gh.analizar                           # resultados.json + figuras
```

## 6. Demostrar la recuperación (lo que hace `orquestar.py recuperacion`)

```powershell
$env:CORRIDA = "rec"
docker compose up -d spark
docker compose run -d --rm productor python -m streaming_gh.productor --corrida rec `
    --desde 2025-09-17T00:00:00Z --hasta 2025-09-17T18:00:00Z --factor 240
Start-Sleep 60;  docker kill streaming-github-redpanda-spark-spark-1            # muerte súbita (SIGKILL), sin apagado limpio
Start-Sleep 20;  docker compose up -d spark                              # reanuda desde los checkpoints en MinIO
# ... cuando el productor termine y plata alcance:
docker compose run --rm tareas python -m streaming_gh.verificar --corrida rec
```

`verificar` compara productor ↔ Kafka ↔ bronce ↔ plata ↔ oro ↔ oro recalculado en lote desde plata y devuelve
`"exactamente_una_vez": true` solo si no hay ni un duplicado ni un evento perdido.

**Por qué funciona.** Cada consulta guarda en su checkpoint (en MinIO) qué offsets/versiones va a procesar en el
lote N *antes* de procesarlo, y lo marca como hecho *después*. Si muere en medio, al reanudar repite el lote N
con exactamente las mismas entradas. La escritura en Delta usa `txnAppId` (la capa) + `txnVersion` (N): si el
lote N ya había quedado escrito, Delta ve que esa transacción ya existe y no escribe nada (el pipeline lo registra
como `escritura_omitida_por_txn` en `lotes.jsonl`). El estado de deduplicación y de las ventanas vive en RocksDB
y se respalda en el checkpoint en cada lote.

## 7. Consultar oro con DuckDB

Sin Spark: DuckDB lee el log de Delta directamente desde MinIO.

```powershell
docker compose run --rm tareas python -m streaming_gh.consultas_oro --corrida principal
```

Las consultas están en `sql/` (repos en tendencia, top por día con `ROW_NUMBER`, lenguajes, actividad por hora,
tipos de evento, picos). Para una sesión interactiva:

```powershell
docker compose run --rm tareas python -c "from streaming_gh.consultas_oro import conectar; c = conectar('principal'); print(c.sql('SELECT * FROM picos_repos ORDER BY estrellas DESC LIMIT 10'))"
```

## 8. Tablero

```powershell
$env:CORRIDA = "principal"; docker compose up -d dashboard       # http://localhost:8501
```

Lee oro cada 15 s con DuckDB: si el pipeline está corriendo, el tablero se mueve solo.

## 9. Apagar

```powershell
docker compose down          # apaga y borra contenedores; los datos quedan en los volúmenes redpanda-datos y minio-datos
docker compose down -v       # además borra los volúmenes (temas y tablas)
```

## Problemas comunes

| Síntoma | Causa y solución |
|---|---|
| `pull access denied for minio/minio` | MinIO dejó de publicar imágenes a fines de 2025. Por eso `docker/minio/Dockerfile` compila el servidor y `mc` desde el código fuente oficial (`docker compose build minio`, ~3 min). |
| 403 al descargar de GH Archive | Cloudflare bloquea el User-Agent por defecto de `urllib`; `descargar_datos.py` manda uno propio. |
| `UnknownTopicOrPartitionException` al arrancar Spark | El tema no existía. El pipeline ahora lo crea (`crear_tema`) antes de suscribirse. |
| Docker Desktop no arranca tras un reinicio (`docker-secrets-engine/engine.sock`) | Socket viejo bloqueado; volver a abrir Docker Desktop lo resolvió. |
| `UnicodeEncodeError: 'charmap'` en scripts del anfitrión | Windows usa cp1252 con la salida redirigida; `orquestar.py` hace `sys.stdout.reconfigure(encoding="utf-8")`. |
| `ProcessingTimeExecutor: Current batch is falling behind` | El micro-lote tardó más que el trigger. Normal al ponerse al día tras una caída o a máxima velocidad. |

## Llevarlo a la nube (sin desplegarlo)

El diseño no cambia; cambian los servicios. `infra/terraform/aws/` tiene la versión AWS como código de
referencia, validada con `terraform fmt -check` + `terraform validate` (salida en `reports/terraform_validate.txt`)
y **nunca aplicada**.

| Local | AWS (Terraform incluido) | Confluent + Databricks |
|---|---|---|
| Redpanda | **MSK Serverless** (Kafka, auth IAM) o Kinesis Data Streams | Confluent Cloud (tema, Schema Registry) |
| Productor Python | el mismo, en ECS/Lambda, con `aws-msk-iam-sasl-signer` | el mismo, con API key de Confluent |
| Spark en contenedor | **EMR Serverless** (emr-7.x, Spark 3.5) o Glue Streaming | Databricks Jobs + Auto Loader / `readStream` de Kafka |
| MinIO + Delta | **S3** (versionado, KMS, ciclo de vida para bronce) + Delta | Unity Catalog + Delta en S3/ADLS |
| Checkpoints en MinIO | checkpoints en S3 | checkpoints en DBFS/Volumes |
| `txnAppId/txnVersion` | igual | igual (o `MERGE` idempotente) |
| DuckDB sobre oro | **Athena** con la base de Glue `streaming_github_oro` | Databricks SQL |
| Streamlit | QuickSight o el mismo Streamlit en App Runner | Databricks SQL Dashboards / Lakeview |
| `docker kill` | caída de un ejecutor o del driver; EMR reintenta el job | reintentos del job |

Diferencias a tener en cuenta:

1. **Escrituras concurrentes en S3.** Delta en S3 necesita un *LogStore* que garantice que dos escritores no
   pisen el mismo commit (`S3DynamoDBLogStore` o, en Delta ≥ 3.3 con S3 *conditional writes*, nada extra). Aquí
   cada tabla tiene un solo escritor, así que el *LogStore* por defecto basta.
2. **Seguridad.** Sin claves: el job asume un rol de IAM (`aws_iam_role.job_spark`) con acceso solo al bucket, a
   los temas de MSK y a Glue. MinIO usa `minioadmin` porque es local.
3. **Costo.** (Cifras en el README, sección «Costo estimado en la nube».) MSK Serverless cobra por clúster-hora + particiones + GB; EMR Serverless por vCPU/GB-hora mientras
   corre (`auto_stop` a los 15 min). Un pipeline 24/7 conviene en EMR on EC2/EKS o Databricks con instancias
   reservadas; uno intermitente, serverless.
4. **Escala.** Las 6 particiones del tema limitan el paralelismo de lectura a 6 tareas: en la nube se suben las
   particiones (y `maxOffsetsPerTrigger`) junto con los ejecutores.

## Preguntas de entrevista (con respuesta)

**1. ¿Qué garantiza "exactamente una vez" aquí y cómo lo demostraste?**
Tres piezas: (a) la fuente es *reproducible* (Kafka guarda los offsets y el checkpoint de Spark dice qué rango
toca a cada lote); (b) el destino es *idempotente* (Delta con `txnAppId` + `txnVersion` ignora un lote que ya
escribió); (c) el estado (dedup y ventanas) se respalda en el checkpoint en cada lote. Lo demostré matando el
contenedor de Spark con `docker kill` tres veces a mitad de la carga y comparando conteos: mensajes en Kafka =
filas en bronce, ids únicos del productor = filas de plata, sin duplicados en ninguna capa (ver README).

**2. ¿Qué es el watermark y qué pasa con un evento que llega tarde?**
Es cuánto se espera a los eventos atrasados en tiempo de evento: `watermark = máximo created_at visto − 10 min`.
Una ventana se cierra (y se escribe en oro, una sola vez, en modo *append*) cuando el watermark pasa su fin; un
evento más viejo que el watermark se descarta en oro. En plata el watermark es de 2 h porque ahí solo sirve para
olvidar ids ya vistos: así un tardío de 30 min sigue entrando a plata (no se pierde el dato crudo tipado) aunque
ya no cuente en la ventana de oro.

**3. Encontraste que un tardío "debía" descartarse y no se descartó. ¿Por qué?**
Desde Spark 3.4, con varios operadores con estado, cada micro-lote filtra los tardíos con el watermark del lote
*anterior* y desaloja estado con el actual. Además, con un trigger de 10 s y el reloj acelerado 120x, un lote
cubre 20 minutos de tiempo de evento. La tolerancia efectiva es entonces el watermark más uno o dos lotes. La
prueba `test_watermark_descarta_los_tardios_fuera_y_acepta_los_de_dentro` fija ese comportamiento con lotes
controlados.

**4. ¿Por qué `dropDuplicatesWithinWatermark` y no `dropDuplicates`?**
`dropDuplicates(["id"])` en streaming guarda todos los ids para siempre: el estado crece sin límite.
`dropDuplicatesWithinWatermark` (Spark 3.5) recuerda cada id solo mientras su `created_at` esté dentro del
watermark, y no exige que los duplicados traigan exactamente la misma marca de tiempo. Los duplicados reales de un
productor que reintenta llegan segundos después, muy dentro de las 2 h.

**5. ¿Por qué `foreachBatch` si Spark ya tiene un sink Delta exactamente-una-vez?**
Porque en cada lote hago más que escribir: mido la latencia de extremo a extremo con los timestamps de Kafka y, en
oro, detecto picos comparando el lote con la historia de la propia tabla (eso no se puede expresar como una
agregación en streaming). Al usar `foreachBatch` pierdo la garantía automática del sink, así que la recupero con
`txnAppId`/`txnVersion`.

**6. ¿Cómo detectas picos en streaming si las funciones de ventana no se pueden usar en streaming?**
Dentro de `foreachBatch` el lote es un DataFrame normal: lo uno con las últimas 12 ventanas de 5 min (o 24 h por
repo) leídas de la tabla oro y calculo un z-score con `Window.rowsBetween(-12, -1)`. El estado vive en Delta, no en
memoria, así que sobrevive a una caída igual que lo demás.

**7. ¿Por qué Delta Lake y no Iceberg? ¿Y por qué Redpanda?**
Delta trae soporte nativo de `txnAppId/txnVersion` para escrituras idempotentes desde `foreachBatch` y un
paquete pip (`delta-spark`) alineado con Spark 3.5; Iceberg habría necesitado un catálogo (REST/Hive/Glue) para
lo mismo. Redpanda habla el protocolo de Kafka (el conector de Spark no cambia), es un binario sin ZooKeeper ni JVM
y arranca en segundos con 1 GB de memoria.

**8. ¿Cómo mides la latencia de extremo a extremo?**
El productor publica con `CreateTime`, así que el timestamp del mensaje es la hora de publicación. Cada
`foreachBatch` toma la hora en que terminó de escribir en Delta y calcula percentiles de
`t_escrito − ts_kafka` del lote (p50, p95 y máximo). En oro es la *frescura*: desde el último evento de la ventana
hasta que la ventana quedó escrita, que incluye la espera del watermark.

**9. ¿Qué limita el rendimiento y qué harías para escalar?**
En local, los 4 núcleos que comparten 5 consultas y la escritura de archivos pequeños en MinIO. Para escalar: más
particiones en el tema, ejecutores en un clúster, triggers más largos (lotes más grandes, menos commits) y
compactar (`OPTIMIZE`) las tablas de bronce y plata fuera de línea.

**10. ¿Qué pasa con los archivos pequeños?**
Un lote cada 3 s por capa genera muchos Parquet chicos. Es el precio de la baja latencia. Se mitiga con triggers
más largos donde la latencia no importa (oro va cada 10 s), particionando solo por fecha y con un `OPTIMIZE` +
`VACUUM` programado (en Databricks, *auto-compaction*).

**11. ¿Por qué reducir los eventos en el productor? ¿No era bronce "crudo"?**
Un PullRequestEvent pesa ~20 KB por el objeto `pull_request` completo; el 90 % no se usa. El productor se queda
con el sobre y los campos de negocio (como un conector de origen que proyecta columnas). Bronce sigue guardando el
mensaje tal cual llega al tema, con sus metadatos de Kafka, que es lo que exige la capa cruda.

**12. ¿Cómo lo llevarías a producción?**
MSK + EMR Serverless + S3 + Glue/Athena (Terraform en `infra/terraform/aws`), rol de IAM sin claves, alarmas sobre
el *lag* del consumidor y la latencia p95, `OPTIMIZE` nocturno, y la misma prueba de recuperación como prueba de
aceptación en un entorno de staging.
