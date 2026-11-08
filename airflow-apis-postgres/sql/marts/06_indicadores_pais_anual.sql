-- Indicadores del Banco Mundial en formato ancho: una fila por país y año.
-- Las columnas deben coincidir con config.INDICADORES (lo verifica tests/test_sql.py).
DROP TABLE IF EXISTS marts.indicadores_pais_anual;
CREATE TABLE marts.indicadores_pais_anual AS
SELECT pais_iso3,
       max(pais)                                                       AS pais,
       anio,
       (pais_iso3 = 'LCN')                                             AS es_agregado,
       max(valor) FILTER (WHERE indicador = 'SP.POP.TOTL')             AS poblacion,
       max(valor) FILTER (WHERE indicador = 'SP.URB.TOTL.IN.ZS')       AS poblacion_urbana_pct,
       max(valor) FILTER (WHERE indicador = 'NY.GDP.PCAP.CD')          AS pib_per_capita_usd,
       max(valor) FILTER (WHERE indicador = 'NY.GDP.MKTP.KD.ZG')       AS crecimiento_pib_pct,
       max(valor) FILTER (WHERE indicador = 'NV.AGR.TOTL.ZS')          AS agro_pct_pib,
       max(valor) FILTER (WHERE indicador = 'EG.ELC.ACCS.ZS')          AS acceso_electricidad_pct,
       max(valor) FILTER (WHERE indicador = 'EN.GHG.CO2.PC.CE.AR5')    AS co2_t_per_capita,
       max(valor) FILTER (WHERE indicador = 'SL.UEM.TOTL.ZS')          AS desempleo_pct,
       max(valor) FILTER (WHERE indicador = 'FP.CPI.TOTL.ZG')          AS inflacion_pct,
       max(valor) FILTER (WHERE indicador = 'SI.POV.GINI')             AS gini
FROM staging.indicadores_bm
GROUP BY pais_iso3, anio;
ALTER TABLE marts.indicadores_pais_anual ADD PRIMARY KEY (pais_iso3, anio);
