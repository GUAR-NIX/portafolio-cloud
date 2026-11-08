-- Extremos diarios por país y año, con la estación y la fecha donde ocurrieron.
with d as (
    select o.*, e.nombre_estacion, e.pais
    from {{ ref('int_observaciones_diarias') }} as o
    left join {{ ref('int_estaciones') }} as e using (id_estacion)
)
select
    codigo_pais,
    any_value(pais)                                   as pais,
    anio,
    count(distinct id_estacion)                       as estaciones,
    max(tmax_c)                                       as tmax_max_c,
    arg_max(nombre_estacion, tmax_c)                  as estacion_tmax_max,
    arg_max(fecha, tmax_c)                            as fecha_tmax_max,
    min(tmin_c)                                       as tmin_min_c,
    arg_min(nombre_estacion, tmin_c)                  as estacion_tmin_min,
    arg_min(fecha, tmin_c)                            as fecha_tmin_min,
    max(prcp_mm)                                      as prcp_max_diaria_mm,
    arg_max(nombre_estacion, prcp_mm)                 as estacion_prcp_max,
    arg_max(fecha, prcp_mm)                           as fecha_prcp_max
from d
group by codigo_pais, anio
