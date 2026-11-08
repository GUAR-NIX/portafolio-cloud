-- Normal de referencia por estación y mes: promedio de los meses completos del periodo de referencia.
select
    id_estacion,
    mes,
    avg(tmedia_c)          as tmedia_normal_c,
    count(*)               as anios_normal
from {{ ref('int_clima_mensual_estacion') }}
where anio between {{ var('anio_normal_inicio') }} and {{ var('anio_normal_fin') }}
  and dias_temp >= {{ var('min_dias_mes') }}
group by id_estacion, mes
having count(*) >= {{ var('min_anios_normal') }}
