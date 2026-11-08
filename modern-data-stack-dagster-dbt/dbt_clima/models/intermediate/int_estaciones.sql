select e.*, p.pais
from {{ ref('stg_ghcn__estaciones') }} as e
left join {{ ref('stg_ghcn__paises') }} as p using (codigo_pais)
