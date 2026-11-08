select codigo as codigo_pais, nombre as pais
from {{ source('ghcn', 'ghcn_paises') }}
