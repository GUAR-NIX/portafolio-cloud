-- Indicadores mensuales por país: promedio simple de las estaciones con mes completo (sin ponderar por área).
select
    codigo_pais,
    any_value(pais)                                                              as pais,
    anio,
    mes,
    make_date(anio, mes, 1)                                                      as mes_inicio,
    count(*) filter (where mes_completo_temp)                                    as estaciones_con_temp,
    count(anomalia_tmedia_c)                                                     as estaciones_con_anomalia,
    avg(tmedia_c) filter (where mes_completo_temp)                               as tmedia_c,
    avg(anomalia_tmedia_c)                                                       as anomalia_tmedia_c,
    median(prcp_total_mm) filter (where dias_prcp >= {{ var('min_dias_mes') }})  as prcp_mensual_mediana_mm,
    max(tmax_extrema_c)                                                          as tmax_extrema_c,
    min(tmin_extrema_c)                                                          as tmin_extrema_c
from {{ ref('mart_clima_mensual_estacion') }}
group by codigo_pais, anio, mes
