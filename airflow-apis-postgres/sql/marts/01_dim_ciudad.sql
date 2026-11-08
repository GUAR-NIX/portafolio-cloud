-- Dimensión de ciudades (13 ciudades, 4 regiones) con la elevación real de la malla del modelo
DROP TABLE IF EXISTS marts.dim_ciudad;
CREATE TABLE marts.dim_ciudad AS
SELECT c.ciudad, c.provincia, c.region, c.latitud, c.longitud,
       e.elevacion_m,
       CASE WHEN e.elevacion_m >= 2000 THEN 'Altura (2.000 m o más)'
            WHEN e.elevacion_m >= 500 THEN 'Media (500 a 2.000 m)'
            ELSE 'Baja (menos de 500 m)' END AS piso_altitudinal
FROM staging.ciudades c
LEFT JOIN (SELECT ciudad, max(elevacion_m) AS elevacion_m FROM staging.clima_diario GROUP BY ciudad) e
       USING (ciudad);
ALTER TABLE marts.dim_ciudad ADD PRIMARY KEY (ciudad);
