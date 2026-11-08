select
    id                               as id_estacion,
    substr(id, 1, 2)                 as codigo_pais,
    nombre                           as nombre_estacion,
    latitud,
    longitud,
    nullif(elevacion_m, -999.9)      as elevacion_m,   -- NOAA usa -999.9 para "sin dato"
    estado,
    wmo_id
from {{ source('ghcn', 'ghcn_estaciones') }}
