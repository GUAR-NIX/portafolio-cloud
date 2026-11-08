#!/usr/bin/env sh
# Prueba de integración: levanta el lakehouse, corre tests/test_integracion.py dentro del cargador y deja el
# compose arriba (bajarlo con `docker compose down`). Uso: sh scripts/prueba_integracion.sh
set -e
docker compose up -d --wait
docker compose run --rm cargador pytest -m integracion -p no:cacheprovider -q
