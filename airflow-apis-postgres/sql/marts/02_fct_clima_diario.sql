-- Hechos diarios por ciudad, con banderas listas para contar en un tablero
DROP TABLE IF EXISTS marts.fct_clima_diario;
CREATE TABLE marts.fct_clima_diario AS
SELECT s.fecha,
       s.ciudad,
       d.region,
       extract(year FROM s.fecha)::int                        AS anio,
       extract(month FROM s.fecha)::int                       AS mes,
       date_trunc('month', s.fecha)::date                     AS mes_inicio,
       extract(isodow FROM s.fecha)::int                      AS dia_semana,
       s.temp_max_c, s.temp_min_c, s.temp_media_c,
       round(s.temp_max_c - s.temp_min_c, 2)                  AS amplitud_termica_c,
       s.precipitacion_mm, s.horas_precipitacion, s.viento_max_kmh,
       s.radiacion_mj_m2, s.evapotranspiracion_mm, s.humedad_media_pct,
       round(s.precipitacion_mm - s.evapotranspiracion_mm, 2) AS balance_hidrico_mm,
       (s.precipitacion_mm >= 1)                              AS dia_lluvioso,
       (s.precipitacion_mm >= 20)                             AS lluvia_fuerte,
       (s.temp_max_c >= 30)                                   AS dia_caluroso,
       s.fuente
FROM staging.clima_diario s
JOIN staging.ciudades d USING (ciudad);
ALTER TABLE marts.fct_clima_diario ADD PRIMARY KEY (fecha, ciudad);
