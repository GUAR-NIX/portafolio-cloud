-- Clima mensual por estación con su anomalía frente a la normal de referencia.
-- Un mes es "completo" si tiene al menos min_dias_mes días con TMAX y TMIN y ya terminó en los datos
-- (el mes en curso del año actual queda fuera: compararlo con la normal del mes entero lo sesga).
with corte as (
    select max(fecha) as ultima_fecha from {{ ref('int_observaciones_diarias') }}
)
select
    m.id_estacion,
    e.nombre_estacion,
    m.codigo_pais,
    e.pais,
    e.latitud,
    e.longitud,
    m.anio,
    m.mes,
    make_date(m.anio, m.mes, 1)                              as mes_inicio,
    m.dias_temp,
    m.dias_temp >= {{ var('min_dias_mes') }}
        and last_day(make_date(m.anio, m.mes, 1)) <= corte.ultima_fecha as mes_completo_temp,
    m.tmax_media_c,
    m.tmin_media_c,
    m.tmedia_c,
    m.dias_prcp,
    m.prcp_total_mm,
    m.tmax_extrema_c,
    m.tmin_extrema_c,
    m.prcp_max_diaria_mm,
    n.tmedia_normal_c,
    n.anios_normal,
    case when m.dias_temp >= {{ var('min_dias_mes') }}
              and last_day(make_date(m.anio, m.mes, 1)) <= corte.ultima_fecha
         then m.tmedia_c - n.tmedia_normal_c end as anomalia_tmedia_c
from {{ ref('int_clima_mensual_estacion') }} as m
left join {{ ref('int_normal_mensual_estacion') }} as n using (id_estacion, mes)
left join {{ ref('int_estaciones') }} as e using (id_estacion)
cross join corte
