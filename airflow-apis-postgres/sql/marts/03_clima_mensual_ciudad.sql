-- Agregado mensual por ciudad: la tabla que más usa un tablero de clima
DROP TABLE IF EXISTS marts.clima_mensual_ciudad;
CREATE TABLE marts.clima_mensual_ciudad AS
SELECT mes_inicio, anio, mes, ciudad, region,
       count(*)                                      AS dias_con_dato,
       round(avg(temp_media_c), 2)                   AS temp_media_c,
       max(temp_max_c)                               AS temp_max_absoluta_c,
       min(temp_min_c)                               AS temp_min_absoluta_c,
       round(avg(amplitud_termica_c), 2)             AS amplitud_media_c,
       round(sum(precipitacion_mm), 2)               AS lluvia_total_mm,
       count(*) FILTER (WHERE dia_lluvioso)          AS dias_lluviosos,
       count(*) FILTER (WHERE lluvia_fuerte)         AS dias_lluvia_fuerte,
       count(*) FILTER (WHERE dia_caluroso)          AS dias_calurosos,
       round(avg(humedad_media_pct), 1)              AS humedad_media_pct,
       round(sum(balance_hidrico_mm), 2)             AS balance_hidrico_mm,
       count(*) FILTER (WHERE fuente = 'pronostico') AS dias_provisionales
FROM marts.fct_clima_diario
GROUP BY mes_inicio, anio, mes, ciudad, region;
ALTER TABLE marts.clima_mensual_ciudad ADD PRIMARY KEY (mes_inicio, ciudad);
