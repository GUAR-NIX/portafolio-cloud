#!/bin/bash
# Se ejecuta una sola vez, al crear el volumen de Postgres. Usuario y bases del almacén de datos
# (separado de los metadatos de Airflow). Credenciales solo para el entorno local de Docker.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    CREATE ROLE datos LOGIN PASSWORD 'datos';
    CREATE DATABASE datos OWNER datos;
    CREATE DATABASE datos_pruebas OWNER datos;
SQL
