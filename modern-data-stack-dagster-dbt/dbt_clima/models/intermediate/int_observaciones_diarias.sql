-- Pivot: una fila por estación y día con TMAX, TMIN y PRCP en columnas.
-- Filtro de calidad: un valor con QFLAG no vacía (falló un control de NOAA) se descarta.
with obs as (
    select * from {{ ref('stg_ghcn__observaciones') }}
)
select
    id_estacion,
    any_value(codigo_pais)                                                       as codigo_pais,
    fecha,
    any_value(anio)                                                              as anio,
    max(valor) filter (where elemento = 'TMAX' and bandera_calidad is null)      as tmax_c,
    max(valor) filter (where elemento = 'TMIN' and bandera_calidad is null)      as tmin_c,
    max(valor) filter (where elemento = 'PRCP' and bandera_calidad is null)      as prcp_mm,
    count(*) filter (where bandera_calidad is not null)                          as valores_descartados_qflag
from obs
group by id_estacion, fecha
