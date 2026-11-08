-- Capas raw -> staging -> marts, más auditoría. Idempotente: se ejecuta antes de cada carga.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS auditoria;

-- raw: la respuesta JSON completa, tal cual llegó (una por partición). Re-ejecutar una fecha la reemplaza.
CREATE TABLE IF NOT EXISTS raw.clima_respuestas (
    fecha           date        PRIMARY KEY,
    fuente          text        NOT NULL CHECK (fuente IN ('archivo', 'pronostico')),
    url             text        NOT NULL,
    parametros      jsonb       NOT NULL,
    payload         jsonb       NOT NULL,
    sha256          text        NOT NULL,
    bytes           integer     NOT NULL,
    descargas       integer     NOT NULL DEFAULT 1,
    descargado_en   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.banco_mundial_respuestas (
    anio            integer     NOT NULL,
    indicador       text        NOT NULL,
    payload         jsonb       NOT NULL,
    registros       integer     NOT NULL,
    sha256          text        NOT NULL,
    descargas       integer     NOT NULL DEFAULT 1,
    descargado_en   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (anio, indicador)
);

-- staging: tipado y una fila por grano natural
CREATE TABLE IF NOT EXISTS staging.ciudades (
    ciudad      text PRIMARY KEY,
    provincia   text NOT NULL,
    region      text NOT NULL,
    latitud     double precision NOT NULL,
    longitud    double precision NOT NULL
);

CREATE TABLE IF NOT EXISTS staging.clima_diario (
    fecha                   date    NOT NULL,
    ciudad                  text    NOT NULL,
    fuente                  text    NOT NULL,
    temp_max_c              numeric(5, 2),
    temp_min_c              numeric(5, 2),
    temp_media_c            numeric(5, 2),
    precipitacion_mm        numeric(7, 2),
    horas_precipitacion     numeric(4, 1),
    viento_max_kmh          numeric(6, 2),
    radiacion_mj_m2         numeric(6, 2),
    evapotranspiracion_mm   numeric(6, 2),
    humedad_media_pct       numeric(5, 1),
    latitud_malla           double precision,
    longitud_malla          double precision,
    elevacion_m             numeric(7, 1),
    cargado_en              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (fecha, ciudad)
);

CREATE TABLE IF NOT EXISTS staging.indicadores_bm (
    pais_iso3           text    NOT NULL,
    pais                text    NOT NULL,
    indicador           text    NOT NULL,
    nombre_indicador    text    NOT NULL,
    anio                integer NOT NULL,
    valor               double precision NOT NULL,
    cargado_en          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pais_iso3, indicador, anio)
);

-- auditoría: cada carga y cada control, para medir el pipeline y armar un tablero de calidad
CREATE TABLE IF NOT EXISTS auditoria.cargas (
    id              bigserial PRIMARY KEY,
    dag_id          text NOT NULL,
    run_id          text NOT NULL,
    tarea           text NOT NULL,
    capa            text NOT NULL,
    particion       text NOT NULL,
    filas           integer NOT NULL,
    segundos        numeric(10, 3) NOT NULL,
    detalle         jsonb,
    registrado_en   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS auditoria.controles_calidad (
    id              bigserial PRIMARY KEY,
    dag_id          text NOT NULL,
    run_id          text NOT NULL,
    capa            text NOT NULL,
    particion       text NOT NULL,
    control         text NOT NULL,
    critico         boolean NOT NULL,
    valor           double precision,
    minimo          double precision,
    maximo          double precision,
    ok              boolean NOT NULL,
    registrado_en   timestamptz NOT NULL DEFAULT now()
);
