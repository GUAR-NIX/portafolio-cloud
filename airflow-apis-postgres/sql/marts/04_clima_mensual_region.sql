-- Región natural por mes: promedio entre ciudades (cada ciudad pesa igual)
DROP TABLE IF EXISTS marts.clima_mensual_region;
CREATE TABLE marts.clima_mensual_region AS
SELECT mes_inicio, anio, mes, region,
       count(DISTINCT ciudad)            AS ciudades,
       round(avg(temp_media_c), 2)       AS temp_media_c,
       round(avg(lluvia_total_mm), 2)    AS lluvia_media_por_ciudad_mm,
       round(avg(dias_lluviosos), 1)     AS dias_lluviosos_promedio
FROM marts.clima_mensual_ciudad
GROUP BY mes_inicio, anio, mes, region;
ALTER TABLE marts.clima_mensual_region ADD PRIMARY KEY (mes_inicio, region);
