-- Ficha por ciudad para todo el período cargado: promedios, extremos y el día en que ocurrieron
DROP TABLE IF EXISTS marts.resumen_ciudad;
CREATE TABLE marts.resumen_ciudad AS
WITH base AS (
    SELECT ciudad, region,
           min(fecha) AS desde, max(fecha) AS hasta, count(*) AS dias,
           round(avg(temp_media_c), 2) AS temp_media_c,
           round(avg(amplitud_termica_c), 2) AS amplitud_media_c,
           round(sum(precipitacion_mm), 1) AS lluvia_total_mm,
           round(100.0 * count(*) FILTER (WHERE dia_lluvioso) / count(*), 1) AS pct_dias_lluviosos,
           count(*) FILTER (WHERE dia_caluroso) AS dias_calurosos
    FROM marts.fct_clima_diario
    GROUP BY ciudad, region
),
mas_calido AS (
    SELECT DISTINCT ON (ciudad) ciudad, fecha AS fecha_max_temp, temp_max_c AS max_temp_c
    FROM marts.fct_clima_diario
    ORDER BY ciudad, temp_max_c DESC, fecha
),
mas_lluvioso AS (
    SELECT DISTINCT ON (ciudad) ciudad, fecha AS fecha_max_lluvia, precipitacion_mm AS max_lluvia_mm
    FROM marts.fct_clima_diario
    ORDER BY ciudad, precipitacion_mm DESC, fecha
)
SELECT b.*,
       round(b.lluvia_total_mm * 365.0 / b.dias, 0) AS lluvia_anualizada_mm,
       c.max_temp_c, c.fecha_max_temp, l.max_lluvia_mm, l.fecha_max_lluvia
FROM base b
JOIN mas_calido c USING (ciudad)
JOIN mas_lluvioso l USING (ciudad);
ALTER TABLE marts.resumen_ciudad ADD PRIMARY KEY (ciudad);
