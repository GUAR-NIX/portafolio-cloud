# Guía paso a paso: modern data stack local con Dagster + dbt + DuckDB + MinIO

## Requisitos

- Docker Desktop (abierto y con `docker info` respondiendo). Nada más: Python, dbt, Dagster y DuckDB
  viven dentro de la imagen `clima-mds:1.0` (Python 3.12, versiones fijadas en `requirements.txt`).
- ~3 GB libres en disco: 0,9 GB de CSV comprimidos en `data/raw/` (no se versionan), el Parquet en el volumen
  de MinIO y el almacén DuckDB en el volumen `almacen`.
- Las únicas credenciales son las de MinIO local (`minio_local` / `minio_local_clave`, en `.env.example`).

## 1. Levantar el stack

```powershell
docker compose build            # imagen clima-mds:1.0 y minio-local (MinIO compilado desde su código fuente)
docker compose up -d            # minio + crear-bucket + dagster-webserver + dagster-daemon
```

- Dagster: <http://localhost:3000> · consola de MinIO: <http://localhost:9001>.
- El webserver ejecuta `dbt parse` al arrancar (genera `manifest.json`, del que dagster-dbt saca los activos);
  el daemon espera a que exista antes de cargar el código.
- Memoria: MinIO 1 GB, webserver 1,5 GB, daemon 5 GB (las corridas se ejecutan en el daemon porque la
  instancia usa `QueuedRunCoordinator` con **una corrida a la vez**: DuckDB admite un solo escritor).

> MinIO dejó de publicar imágenes en Docker Hub a fines de 2025; `docker/minio/Dockerfile` compila la última
> versión etiquetada del servidor y de `mc` (licencia AGPL-3.0).

## 2. Backfill por año

Desde la UI: *Assets → ghcn/ghcn_crudo_parquet → Materialize → todas las particiones*, o por consola:

```powershell
docker compose exec dagster-daemon dagster job backfill -w workspace.yaml -j ingesta_anual --partitions 2022,2023,2024,2025,2026 --noprompt
```

Cada partición (un año) descarga el `.csv.gz` de NOAA (reanudable: `.part` + `Range` + `If-Range`), lo convierte
a Parquet zstd ordenado por elemento/estación con DuckDB, lo sube a `s3://ghcn/crudo/ghcn_diario/anio=AAAA/` y
evalúa 5 asset checks bloqueantes. Si la PC se apaga, basta re-ejecutar la partición: lo descargado se reutiliza.

## 3. Transformaciones dbt

```powershell
docker compose exec dagster-daemon dagster job launch -w workspace.yaml -j transformaciones_dbt
```

El job refresca los metadatos (estaciones y países) y ejecuta `dbt build` desde Dagster (modelos + pruebas; cada
prueba de dbt aparece como asset check). Comandos de dbt directos:

```powershell
docker compose exec dagster-daemon sh -c "cd dbt_clima && dbt source freshness --profiles-dir ."
docker compose exec dagster-daemon sh -c "cd dbt_clima && dbt docs generate --profiles-dir ."
```

## 4. Mediciones, figuras y muestra

```powershell
docker compose exec dagster-daemon python scripts/analizar.py       # reports/*.json|csv y reports/figures
docker compose exec dagster-daemon python scripts/crear_muestra.py  # data/muestra (CI)
.\scripts\capturas_ui.ps1                                           # reports/capturas (Chrome sin ventana)
```

## 5. Pruebas (igual que la CI)

```powershell
docker compose run --rm --no-deps -e DBT_TARGET=ci -e UBICACION_CRUDO=/opt/proyecto/data/muestra dagster-webserver sh -c "cd dbt_clima && dbt build --profiles-dir . && cd .. && ruff check src tests scripts descargar_datos.py && pytest -rs"
```

## 6. Demostración de un asset check que falla

En la UI, *Materialize* de la partición 2025 de `ghcn/ghcn_crudo_parquet` con esta configuración:

```yaml
ops:
  ghcn__ghcn_crudo_parquet:
    config:
      simular_datos_malos: true
```

`simular_datos_malos` multiplica a 999,9 °C ~2 % de las TMAX. El check `temperatura_en_rango_fisico` falla
(bloqueante), la corrida queda en rojo y nada aguas abajo se recalcula con esos datos. Re-materializar sin la
configuración deja todo en verde otra vez.

## 7. Terraform de referencia (GCP, sin aplicar)

```powershell
docker run --rm -v "${PWD}/terraform/gcp:/tf" -w /tf hashicorp/terraform:1.13 fmt -check -recursive
docker run --rm -v "${PWD}/terraform/gcp:/tf" -w /tf hashicorp/terraform:1.13 init -backend=false
docker run --rm -v "${PWD}/terraform/gcp:/tf" -w /tf hashicorp/terraform:1.13 validate
```

Nunca `plan` ni `apply`: el proyecto `proyecto-ficticio-clima` no existe y no hay credenciales.

## 8. Apagar

```powershell
docker compose down             # los datos quedan en los volúmenes minio-datos, almacen y dagster-home
```

---

## Preguntas de entrevista

**1. ¿Por qué dbt y no scripts SQL sueltos?**
dbt convierte el SQL en un proyecto de software: cada modelo es un `SELECT` versionado, `ref()` arma el grafo de
dependencias (y el orden de ejecución) solo, las pruebas viven junto al modelo y `dbt docs` publica el linaje y
las descripciones. Cambiar de DuckDB a BigQuery es cambiar el perfil, no reescribir el pipeline.

**2. ¿Qué diferencia hay entre orquestar activos (Dagster) y orquestar tareas (Airflow clásico)?**
En un DAG de tareas el orquestador sabe que "la tarea B corre después de A"; en Dagster declara *qué datos
existen* (`ghcn/ghcn_crudo_parquet`, `marts/mart_clima_mensual_pais`) y de cuáles dependen. Con eso la UI muestra
el linaje, qué está desactualizado, qué partición falta y qué checks pasaron; un backfill es "materializar estas
particiones", no "volver a correr estas fechas". Airflow 3 se acerca con *assets*, pero su unidad sigue siendo la tarea.

**3. ¿Cómo se integra dbt en Dagster?**
`dagster-dbt` lee `manifest.json` y crea un activo por modelo y un asset check por prueba. Las fuentes de dbt se
enlazan con los activos de Python por su clave (`ghcn/ghcn_crudo_parquet`), así el grafo va desde la descarga
hasta los marts sin huecos. La función `@dbt_assets` llama a `dbt build` con la selección que pidió el usuario.

**4. ¿Por qué particionar por año y no por día?**
NOAA publica un archivo por año; la partición coincide con la unidad que se descarga y se re-procesa. Los años
cerrados casi no cambian; el año en curso se refresca a diario con el schedule `ingesta_diaria_anio_en_curso`.
Con particiones diarias habría 1.800 corridas que leen el mismo archivo.

**5. ¿Por qué Parquet en MinIO y no cargar el CSV directo a DuckDB?**
Separar almacenamiento y cómputo, como en la nube: el lago crudo (S3/GCS) es barato y durable, y cualquier motor
lo lee (DuckDB, Spark, BigQuery, Athena). Parquet es columnar, tipado y comprimido: aquí el CSV de 1,37 GB por
año queda en ~37 MB y las consultas solo leen las columnas y los grupos de filas que necesitan.

**6. ¿Qué pruebas de calidad hay y dónde?**
En la ingesta, 5 asset checks bloqueantes por partición (volumen, fechas del año, nulos, elementos presentes,
rango físico). En dbt: `not_null`, `unique`, `accepted_values`, `relationships`, pruebas propias de rango físico,
clave compuesta y expresión, una conciliación entre marts y la *freshness* de la fuente. Encima, un check de
negocio en Dagster sobre el mart por país. Las que pueden fallar por particularidades de NOAA (inversión
TMAX < TMIN) tienen severidad `warn`.

**7. ¿Qué es la normal climatológica y por qué la tuya es distinta?**
La OMS usa promedios de 30 años (1991-2020). Con 5 años descargados, la "normal" es el promedio 2022-2024 por
estación y mes; sirve para comparar 2025-2026 fuera de la muestra, pero no es comparable con anomalías oficiales.
Ampliarla es cambiar `ANIOS` y las variables `anio_normal_*`: el pipeline no cambia.

**8. ¿Por qué la anomalía se calcula por estación y no restando promedios del país?**
Porque la red cambia: estaciones que se suman o dejan de reportar mueven el promedio del país aunque el clima no
cambie. Restar a cada estación su propia normal elimina ese sesgo de composición; la línea base ingenua del
README lo cuantifica.

**9. ¿Cómo lo escalarías a la nube?**
`terraform/gcp`: el bucket de MinIO pasa a Cloud Storage, DuckDB a BigQuery (mismos modelos con `dbt-bigquery`),
el daemon a un Cloud Run job y el schedule a Cloud Scheduler, con cuentas de servicio de mínimo privilegio.
Con más volumen: particiones diarias o por país, Dagster con Postgres y ejecución en Kubernetes o Dagster+.

**10. ¿Qué pasa si la PC se apaga a mitad del backfill?**
La descarga queda en `.part` y se reanuda con `Range`; el Parquet se sobreescribe completo (idempotente); el
estado de Dagster vive en un volumen. Se re-lanzan las particiones fallidas desde la UI y las terminadas no se tocan.

**11. ¿Por qué una sola corrida a la vez?**
DuckDB es un archivo con un único escritor y la PC tiene que compartir RAM con otros proyectos. En la nube el
almacén (BigQuery) admite concurrencia y el límite se sube.

**12. ¿Cómo evitas versionar datos o credenciales?**
`.gitignore` deja fuera `data/raw/`, `*.duckdb` y `target/`. Lo único versionado es una muestra de ~1 MB para la
CI. Las credenciales son de MinIO local y se pasan por variables de entorno; Terraform no tiene secretos.
