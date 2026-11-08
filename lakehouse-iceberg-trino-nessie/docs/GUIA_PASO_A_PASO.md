# Guía paso a paso: lakehouse con Iceberg, Trino y Nessie

Esta guía explica cómo está armado el proyecto, en qué orden se ejecuta y por qué se tomó cada decisión. Al final hay
preguntas de entrevista con respuesta.

## 1. Conceptos en una página

- **Formato de tabla abierto (Iceberg).** Los datos son archivos Parquet normales en un almacenamiento de objetos. Iceberg
  agrega una capa de **metadatos**: un archivo `metadata.json` con el esquema, las especificaciones de partición y el
  snapshot vigente. Ese snapshot apunta a una lista de manifiestos y cada manifiesto lista archivos de datos con sus
  estadísticas (filas, min/max por columna). Un commit es un cambio atómico del puntero al `metadata.json` nuevo.
- **Catálogo.** Guarda ese puntero para cada tabla. Aquí es **Nessie**, que además versiona todos los punteros
  juntos como un repositorio git: commits, ramas, etiquetas y merge.
- **Motor.** **Trino** lee y escribe Iceberg desde SQL. **PyIceberg** hace lo mismo desde Python, sin JVM.
- **Lakehouse.** Es almacenamiento barato y abierto (S3/MinIO) con garantías de base de datos: transacciones, esquema,
  time travel y control de concurrencia.

## 2. Levantar el entorno

```bash
docker compose build
docker compose up -d --wait
```

Orden de arranque (con `depends_on` y healthchecks):

1. `minio`: S3 local. La imagen se compila desde el código fuente oficial, porque MinIO dejó de publicar imágenes.
2. `minio-init`: crea el bucket `lakehouse`.
3. `nessie`: catálogo con RocksDB en un volumen. Las variables `nessie.catalog.*` lo convierten en **catálogo
   Iceberg REST** que escribe metadatos en `s3://lakehouse/warehouse/`.
4. `nessie-init`: crea la rama `dev` y la etiqueta `historia` con `lakehouse/nessie.py`.
5. `trino`: tres catálogos fijos, cada uno con su URI REST:
   `iceberg` → `/iceberg/main`, `iceberg_dev` → `/iceberg/dev` y `iceberg_historia` → `/iceberg/historia`.
6. `cargador` (perfil `herramientas`): se usa con `docker compose run --rm cargador python scripts/...`.

Límites de memoria: Trino 3 GB (heap de 2 GB), MinIO 1 GB, Nessie 1 GB y cargador 2 GB.

## 3. Descargar y cargar

`scripts/cargar.py 2026-07-18 2026-07-19 --rama main` hace, para cada hora:

1. **Descarga** (`lakehouse/descarga.py`). Lee el `.gz` en streaming y guarda solo `en`, `en.m`, `es` y `es.m`
   con ≥ 5 vistas en `data/pageviews/` (~2,7 MB por hora). Escribe en `*.part`, renombra al terminar y deja un
   manifiesto con sha256. Una hora ya bajada y verificada no se vuelve a pedir. Usa una sola conexión: mientras se
   procesa una hora se baja la siguiente.
2. **Idempotencia**: si la hora ya está en la columna `hora` de la tabla en esa rama, se salta.
3. **Crudo**: cada línea partida en 4 textos, más el archivo y el número de línea. Partición `day(hora)`.
4. **Limpio** (`lakehouse/limpieza.py`): valida, decodifica `%XX`, pone la primera letra en mayúscula, quita
   páginas que no son artículos y fusiona duplicados. Partición `idioma` + `day(hora)`.
5. Cada `append` es un commit en Nessie con las propiedades `lakehouse.hora` y `lakehouse.lote`.

Si el proceso se corta, se vuelve a lanzar el mismo comando y continúa donde quedó.

Wikimedia puede volver a publicar un dump ya existente: los de julio de 2026 se regeneraron entre el 31-oct y el
1-nov-2026, después de la ejecución de `reports/` (27-oct). Por eso una corrida nueva da cifras unos pocos puntos
porcentuales distintas de las del README (por ejemplo, 15.027.085 filas en `main` tras el 19-jul en lugar de
15.263.513), con las mismas conclusiones. Los dumps no se versionan: la URL es la misma y el contenido cambia.

## 4. Demostraciones de Iceberg

| Paso | Comando | Evidencia |
|---|---|---|
| Time travel | `demos_iceberg.py time_travel` | `reports/iceberg/01_time_travel.json` |
| Esquema | `demos_iceberg.py esquema` | `02_evolucion_esquema.json` |
| Partición (1) | `demos_iceberg.py particiones_antes` | `03a_particiones_antes.json` |
| Carga del 20 | `cargar.py 2026-07-20 --rama main` | `reports/carga/2026-07-20_main.json` |
| Partición (2) | `demos_iceberg.py particiones_despues` | `03b_particiones_despues.json` |
| Compactación | `demos_iceberg.py compactacion` → `nessie-gc` → `compactacion_final` | `04_compactacion.json`, `logs/nessie_gc.log` |
| Ramas | `ramas.py` | `05_ramas_nessie.json` |

El comando de `nessie-gc` (se corre desde el host, en la red del compose):

```bash
docker run --rm --network lakehouse-iceberg_default ghcr.io/projectnessie/nessie-gc:0.108.8 gc --inmemory -u http://nessie:19120/api/v2 --default-cutoff PT0S -I s3.endpoint=http://minio:9000 -I s3.path-style-access=true -I client.region=us-east-1 -I s3.access-key-id=minioadmin -I s3.secret-access-key=minioadmin
```

`--default-cutoff PT0S` significa que solo la cabeza de cada rama o etiqueta está viva. En producción se usa algo
como `P7D`, para conservar 7 días de time travel.

### Cómo funciona el write-audit-publish (`scripts/ramas.py`)

1. `Nessie.reasignar("dev", "main")`: `dev` queda igual que `main`.
2. Cargar el día en `dev` (las tablas de `main` no cambian).
3. Métricas en Trino sobre `iceberg_dev` (horas, duplicados, nulos, no artículos, vistas por idioma) y referencia
   con los días de `main`.
4. `controles.evaluar()` → si todo pasa, `Nessie.fusionar("dev", "main")`; si no, `dev` se reasigna a `main` y los
   datos malos se descartan sin haber llegado nunca a `main`.

## 5. Agregados, rendimiento y análisis

```bash
docker compose run --rm cargador python scripts/agregados.py      # sql/agregados.sql + verificación del corte
docker compose run --rm cargador python scripts/rendimiento.py    # 6 casos × 5 corridas
docker compose run --rm cargador python scripts/analisis.py       # CSV de negocio + 3 figuras
docker compose run --rm cargador python scripts/analisis.py tecnicas
python scripts/costo_nube.py
```

## 6. Pruebas

- `pytest -q` (32 pruebas, sin red ni contenedores): reglas de limpieza y conciliación sobre una muestra real de 400
  líneas; cada defecto hace fallar exactamente su control; la expresión regular de Trino coincide con la regla de
  Python; descarga con servidor simulado (filtro, sha256, reintento, sin `.part` colgados); cliente de Nessie con
  respuestas simuladas.
- `sh scripts/prueba_integracion.sh`: crea una rama al azar, escribe una tabla con PyIceberg, comprueba que `main`
  no la ve, ve la diferencia en Nessie, fusiona y la lee desde Trino. Después borra todo.
- CI (`../.github/workflows/lakehouse-iceberg-trino-nessie.yml`): ruff + pytest, Terraform `fmt`/`validate` y, de forma manual, la prueba de humo
  con el compose.

## 7. Problemas encontrados y cómo se resolvieron

- **El catálogo REST de Nessie guarda un solo snapshot por tabla.** La idempotencia basada en propiedades del
  snapshot dejó de servir y `FOR VERSION AS OF` no encuentra snapshots viejos. Solución: leer las horas de los
  datos y hacer time travel con `rama@hash` o etiquetas.
- **`expire_snapshots` falla con `GC is disabled`.** Nessie fija `gc.enabled=false` a propósito. Se usa `nessie-gc`.
- **PyIceberg elegía `FsspecFileIO`** porque Nessie lo sugiere en la configuración de la tabla; hay que instalar
  `s3fs`. Para escribir con particiones `day`/`hour` hace falta `pyiceberg-core`.
- **`jvm.config` propio**: las opciones de JVM cambian entre versiones de Trino; se partió del archivo de la imagen
  483 y solo se fijó `-Xmx2G`.
- **Poda engañosa en la medición**: `concat(idioma, '')`, `lower(idioma)` y `dominio IN (...)` seguían podando
  (por la partición identidad o por los min/max de archivo). El caso "sin poda" usa funciones sobre columnas que no
  son de partición.
- **Disco**: el disco virtual de Docker no devuelve espacio al host. Por eso los dumps se filtran en streaming y no
  se guardan completos.

## 8. Preguntas de entrevista

**1. ¿Qué problema resuelve un formato de tabla como Iceberg frente a "Parquet en una carpeta"?**
Una carpeta de Parquet no tiene transacciones: un lector puede ver una escritura a medias, dos escritores pueden
pisarse y para saber qué archivos leer hay que listar la carpeta (lento en S3). Iceberg agrega metadatos versionados.
Cada commit cambia atómicamente qué archivos forman la tabla. Los lectores ven un snapshot consistente, y la lista
de archivos con sus estadísticas permite podar sin listar. Además trae evolución de esquema y de partición, y time
travel.

**2. ¿Por qué Iceberg y no Delta Lake o Hudi?**
Por el ecosistema y por la partición oculta. Iceberg tiene un catálogo REST estándar que usan Trino, Spark, Flink,
PyIceberg, Snowflake y Athena. La partición se define como transformación (`hour(hora)`): las consultas filtran por
`hora` y la partición puede cambiar sin reescribir. Delta es excelente dentro de Databricks/Spark, pero su
partición es una columna física. Hudi brilla en upserts de alta frecuencia (CDC), que aquí no hacen falta.

**3. ¿Qué es la evolución de particiones y por qué las consultas viejas siguen funcionando?**
Cada archivo de datos recuerda con qué especificación (`spec_id`) se escribió. Al cambiar de `day(hora)` a
`hour(hora)` los archivos viejos no se tocan. Las consultas filtran por la columna original (`hora`), nunca por la
partición, así que el planificador traduce el filtro para cada spec por separado. En el proyecto, las dos consultas
guardadas antes del cambio dieron exactamente el mismo resultado después.

**4. Una consulta tarda 850 ms y otra igual 75 ms. ¿Qué pasó?**
Poda. Con `hora = TIMESTAMP '...'` Trino descarta particiones y archivos por sus min/max y lee 0,28 MB. Con
`date_format(hora, ...)` el filtro va dentro de una función que el planificador no puede invertir, y lee los 323 MB.
Regla: filtrar sobre la columna cruda y dejar que Iceberg haga la traducción. También se ve que Trino sí poda
particiones identidad aunque haya funciones (`lower(idioma)`).

**5. ¿Por qué compactar y qué costo tiene?**
Cada carga horaria deja archivos pequeños: más archivos significan más solicitudes a S3, más metadatos y más
planificación. `optimize` reescribió 72 archivos de 4 MB en 3 de 73 MB (y comprimió un 29 % mejor). El costo: si
la compactación mezcla horas, una consulta de una hora lee el día entero (65 MB en el crudo compactado). Por eso se
compacta ordenando por la columna de filtro o con particiones acordes. Además, después de compactar los archivos
viejos siguen ahí hasta que corre la limpieza: MinIO pasó de 306 a 525 MB antes de `nessie-gc`.

**6. ¿Por qué falló `expire_snapshots` y qué hiciste?**
Con Nessie un archivo que ya no usa `main` puede seguir usándolo otra rama o etiqueta, y `expire_snapshots` solo ve
una. Por eso Nessie crea las tablas con `gc.enabled=false` y Trino se niega. El equivalente seguro es `nessie-gc`:
recorre todas las referencias, arma el conjunto de archivos vivos (con un filtro de Bloom) y borra el resto. Aquí
borró 434 archivos y dejó 219 MB.

**7. ¿Qué es write-audit-publish y cómo lo implementaste?**
Escribir en un lugar aislado, auditar y publicar de forma atómica solo si pasa. Con Nessie: cargar el día en `dev`,
medir con Trino (24 horas, sin duplicados, sin páginas especiales, volumen dentro de 0,6–1,5× la mediana) y
`merge` a `main` si todo está en verde. El día con defectos falló 3 controles y el hash de `main` no cambió. El
correcto pasó 7/7 y el merge tardó 0,07 s, porque solo mueve punteros sin copiar datos.

**8. ¿Cómo garantizas que una recarga no duplique datos?**
Cada hora es una unidad idempotente. Antes de cargar, el cargador lee qué horas ya están en la columna `hora` de esa
rama y salta las presentes. Además, la descarga se verifica con sha256 y el control "sin duplicados (hora, dominio,
título)" detecta cualquier doble carga antes del merge. Primero se marcaba en las propiedades del snapshot, pero el
catálogo REST de Nessie solo conserva el snapshot vigente; leer los datos es la fuente de verdad.

**9. ¿Cómo hiciste time travel si `FOR VERSION AS OF` falló?**
En Nessie el historial está en los commits del catálogo. Se busca el commit en que la tabla terminaba en la hora
deseada, se mueve la etiqueta `historia` a ese commit y se consulta un catálogo de Trino que apunta a la etiqueta.
Resultado: 7.618.728 filas (solo el 18-jul) frente a 15.263.513 en `main`. Como ventaja, el time travel abarca
todas las tablas a la vez (un commit de Nessie es multi-tabla).

**10. ¿Por qué filtraste a ≥ 5 vistas por hora? ¿No sesga el análisis?**
Para que cupiera en un portátil: de 5,2 GB descargados quedan 262 MB. El filtro se queda con el 5,2 % de las líneas
y el 69,3 % de las vistas es/en. No cambia los tops ni los picos, que son de miles de vistas, pero sí los totales por
idioma, y por eso el README dice que las vistas son "de artículos con ≥ 5 vistas por hora". En producción se
guardaría todo en crudo y el umbral iría en una capa de presentación.

**11. ¿Cómo llevarías esto a AWS y cuánto costaría?**
S3 en lugar de MinIO, Glue Data Catalog en lugar de Nessie y Athena (que es Trino administrado) en lugar de Trino; el
cargador en Fargate o Lambda. Glue no tiene ramas, así que el write-audit-publish se haría con ramas de Iceberg o con
una tabla de staging. Con 1 año de datos y 30.000 consultas de tablero al mes: ~7 USD/mes. Athena es la mayor parte,
así que la poda es lo que más ahorra.

**12. ¿Qué cambiarías para producción?**
`nessie-gc` programado con cutoff de 7 días; Nessie con una base de datos replicada (PostgreSQL o DynamoDB) en lugar
de RocksDB; autenticación (OIDC) en Nessie y Trino; compactación nocturna ordenada por `hora`; las cargas
orquestadas (Airflow o Dagster) con alertas cuando un control falla; y Trino con varios workers.
