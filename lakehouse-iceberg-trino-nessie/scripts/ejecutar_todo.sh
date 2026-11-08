#!/usr/bin/env sh
# Flujo completo, en el orden en que se generó la evidencia de reports/. Uso: sh scripts/ejecutar_todo.sh
# Tarda ~1,5 h (casi todo es la descarga: 96 archivos horarios, uno a la vez). Es reanudable: cada hora ya cargada
# en una rama se salta (propiedad `lakehouse.hora` del snapshot) y cada dump ya filtrado se reutiliza (sha256).
set -e
R="docker compose run --rm -T cargador"
docker compose up -d --wait

$R python scripts/cargar.py 2026-07-18 2026-07-19 --rama main     # crudo + limpio con partición diaria
$R python scripts/demos_iceberg.py time_travel
$R python scripts/demos_iceberg.py esquema                         # ADD COLUMN lote
$R python scripts/demos_iceberg.py particiones_antes               # diaria → horaria
$R python scripts/cargar.py 2026-07-20 --rama main                 # se escribe con la partición nueva
$R python scripts/demos_iceberg.py particiones_despues
$R python scripts/demos_iceberg.py compactacion                    # optimize + expire_snapshots en crudo
# GC de Nessie: recorre todas las ramas y etiquetas y borra los archivos que ningún commit vivo usa
docker run --rm --network lakehouse-iceberg_default --memory 768m ghcr.io/projectnessie/nessie-gc:0.108.8   gc --inmemory -u http://nessie:19120/api/v2 --default-cutoff PT0S   -I s3.endpoint=http://minio:9000 -I s3.path-style-access=true -I client.region=us-east-1   -I s3.access-key-id=minioadmin -I s3.secret-access-key=minioadmin
$R python scripts/demos_iceberg.py compactacion_final
$R python scripts/ramas.py                                         # 21-jul por la rama dev (falla y luego pasa)
$R python scripts/agregados.py
$R python scripts/rendimiento.py
$R python scripts/analisis.py
$R python scripts/analisis.py tecnicas
$R python scripts/descargar.py                                     # solo resume los manifiestos (todo en caché)
python scripts/costo_nube.py
