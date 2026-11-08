-- Conciliación: las estaciones con mes completo del mart por país deben sumar lo mismo que en el mart por estación.
with por_estacion as (
    select count(*) as n from {{ ref('mart_clima_mensual_estacion') }} where mes_completo_temp
),
por_pais as (
    select sum(estaciones_con_temp) as n from {{ ref('mart_clima_mensual_pais') }}
)
select * from por_estacion, por_pais where por_estacion.n <> por_pais.n
