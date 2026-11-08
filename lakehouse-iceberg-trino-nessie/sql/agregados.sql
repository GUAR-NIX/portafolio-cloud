-- Capa de agregados (Trino sobre Iceberg). Se recrea completa con scripts/agregados.py.
-- Las vistas de un artículo se suman entre escritorio y móvil (clave: idioma + título).

-- 1) Vistas por hora, idioma y plataforma (curva diaria, participación móvil)
CREATE OR REPLACE TABLE iceberg.wiki.agg_vistas_hora WITH (partitioning = ARRAY['idioma']) AS
SELECT hora, idioma, plataforma, sum(vistas) AS vistas, count(*) AS articulos
FROM iceberg.wiki.pageviews_limpio
GROUP BY hora, idioma, plataforma;

-- 2) Candidatos: artículos con al menos una fila de >= 50 vistas en alguna hora. Agregar solo candidatos evita
--    agrupar decenas de millones de títulos de la cola larga; scripts/agregados.py verifica que el corte no
--    cambia ningún top (el último puesto de cada top supera lo máximo que podría sumar un no candidato).
CREATE OR REPLACE TABLE iceberg.wiki.agg_candidatos AS
SELECT DISTINCT idioma, titulo
FROM iceberg.wiki.pageviews_limpio
WHERE vistas >= 50;

-- 3) Serie horaria exacta de los candidatos
CREATE OR REPLACE TABLE iceberg.wiki.agg_articulo_hora WITH (partitioning = ARRAY['idioma', 'day(hora)']) AS
SELECT l.hora, l.idioma, l.titulo, sum(l.vistas) AS vistas,
       sum(CASE WHEN l.plataforma = 'movil' THEN l.vistas ELSE 0 END) AS vistas_movil
FROM iceberg.wiki.pageviews_limpio l
JOIN iceberg.wiki.agg_candidatos c ON c.idioma = l.idioma AND c.titulo = l.titulo
GROUP BY l.hora, l.idioma, l.titulo;

-- 4) Top 10 por hora e idioma
CREATE OR REPLACE TABLE iceberg.wiki.agg_top_hora AS
SELECT hora, idioma, puesto, titulo, vistas
FROM (SELECT hora, idioma, titulo, vistas,
             row_number() OVER (PARTITION BY hora, idioma ORDER BY vistas DESC, titulo) AS puesto
      FROM iceberg.wiki.agg_articulo_hora)
WHERE puesto <= 10;

-- 5) Top 50 por día e idioma
CREATE OR REPLACE TABLE iceberg.wiki.agg_top_dia AS
SELECT dia, idioma, puesto, titulo, vistas
FROM (SELECT dia, idioma, titulo, vistas,
             row_number() OVER (PARTITION BY dia, idioma ORDER BY vistas DESC, titulo) AS puesto
      FROM (SELECT CAST(hora AS date) AS dia, idioma, titulo, sum(vistas) AS vistas
            FROM iceberg.wiki.agg_articulo_hora GROUP BY 1, 2, 3))
WHERE puesto <= 50;

-- 6) Picos: vistas de la hora frente al promedio de las 24 horas anteriores (serie densa: hora sin fila = 0).
--    Solo artículos que estuvieron en algún top 10 horario.
CREATE OR REPLACE TABLE iceberg.wiki.agg_picos AS
WITH horas AS (SELECT DISTINCT hora FROM iceberg.wiki.agg_vistas_hora),
     articulos AS (SELECT DISTINCT idioma, titulo FROM iceberg.wiki.agg_top_hora),
     serie AS (
        SELECT h.hora, a.idioma, a.titulo, coalesce(s.vistas, 0) AS vistas
        FROM horas h CROSS JOIN articulos a
        LEFT JOIN iceberg.wiki.agg_articulo_hora s ON s.hora = h.hora AND s.idioma = a.idioma AND s.titulo = a.titulo),
     con_base AS (
        SELECT *, avg(vistas) OVER (PARTITION BY idioma, titulo ORDER BY hora
                                    ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS base_24h,
               count(*) OVER (PARTITION BY idioma, titulo ORDER BY hora
                              ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS horas_base
        FROM serie)
SELECT hora, idioma, titulo, vistas, round(base_24h, 1) AS base_24h,
       round(vistas / (base_24h + 100), 1) AS razon  -- +100 suaviza artículos que venían de casi cero
FROM con_base
WHERE horas_base = 24 AND vistas >= 5000 AND vistas / (base_24h + 100) >= 5;
