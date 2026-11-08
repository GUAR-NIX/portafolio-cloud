-- Ecuador frente a sus pares por indicador y año: valor, promedio de los otros 7 países, agregado
-- de América Latina y el Caribe, puesto de Ecuador (1 = valor más alto) y cambio frente al año anterior
DROP TABLE IF EXISTS marts.ecuador_vs_region;
CREATE TABLE marts.ecuador_vs_region AS
WITH paises AS (
    SELECT indicador, nombre_indicador, anio, pais_iso3, valor,
           rank() OVER (PARTITION BY indicador, anio ORDER BY valor DESC) AS puesto,
           count(*) OVER (PARTITION BY indicador, anio)                   AS paises_con_dato
    FROM staging.indicadores_bm
    WHERE pais_iso3 <> 'LCN'
),
ecuador AS (
    SELECT e.indicador, e.nombre_indicador, e.anio, e.valor, e.puesto, e.paises_con_dato,
           round(avg(p.valor)::numeric, 3) AS promedio_pares
    FROM paises e
    LEFT JOIN paises p ON p.indicador = e.indicador AND p.anio = e.anio AND p.pais_iso3 <> 'ECU'
    WHERE e.pais_iso3 = 'ECU'
    GROUP BY e.indicador, e.nombre_indicador, e.anio, e.valor, e.puesto, e.paises_con_dato
)
SELECT e.indicador, e.nombre_indicador, e.anio,
       e.valor                                                                         AS valor_ecuador,
       e.promedio_pares,
       l.valor                                                                         AS valor_america_latina,
       e.puesto                                                                        AS puesto_ecuador,
       e.paises_con_dato,
       round((e.valor - lag(e.valor) OVER (PARTITION BY e.indicador ORDER BY e.anio))::numeric, 3)
                                                                                       AS cambio_anual
FROM ecuador e
LEFT JOIN staging.indicadores_bm l ON l.indicador = e.indicador AND l.anio = e.anio AND l.pais_iso3 = 'LCN';
ALTER TABLE marts.ecuador_vs_region ADD PRIMARY KEY (indicador, anio);
