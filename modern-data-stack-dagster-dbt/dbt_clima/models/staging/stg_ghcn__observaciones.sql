-- Tipos y unidades: fecha AAAAMMDD -> DATE; décimas de °C y de mm -> °C y mm.
-- Solo los elementos que usa el negocio; el Parquet está ordenado por elemento, así que el filtro salta grupos de filas.
select
    id                                                         as id_estacion,
    substr(id, 1, 2)                                           as codigo_pais,
    make_date(fecha // 10000, (fecha // 100) % 100, fecha % 100) as fecha,
    cast(fecha // 10000 as integer)                            as anio,
    elemento,
    valor                                                      as valor_original,
    valor / 10.0                                               as valor,
    nullif(trim(m_flag), '')                                   as bandera_medicion,
    nullif(trim(q_flag), '')                                   as bandera_calidad,
    nullif(trim(s_flag), '')                                   as bandera_fuente
from {{ source('ghcn', 'ghcn_crudo_parquet') }}
where elemento in ('TMAX', 'TMIN', 'PRCP')
