-- Agregado mensual por estación (base para la normal y para el mart).
select
    id_estacion,
    any_value(codigo_pais)                                                     as codigo_pais,
    anio,
    cast(month(fecha) as integer)                                              as mes,
    count(tmax_c)                                                              as dias_tmax,
    count(tmin_c)                                                              as dias_tmin,
    count(*) filter (where tmax_c is not null and tmin_c is not null)          as dias_temp,
    avg(tmax_c)                                                                as tmax_media_c,
    avg(tmin_c)                                                                as tmin_media_c,
    avg((tmax_c + tmin_c) / 2) filter (where tmax_c is not null and tmin_c is not null) as tmedia_c,
    count(prcp_mm)                                                             as dias_prcp,
    sum(prcp_mm)                                                               as prcp_total_mm,
    max(tmax_c)                                                                as tmax_extrema_c,
    min(tmin_c)                                                                as tmin_extrema_c,
    max(prcp_mm)                                                               as prcp_max_diaria_mm,
    sum(valores_descartados_qflag)                                             as valores_descartados_qflag
from {{ ref('int_observaciones_diarias') }}
group by id_estacion, anio, month(fecha)
