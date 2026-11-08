{# Falla con las combinaciones de columnas que se repiten (clave natural compuesta). #}
{% test combinacion_unica(model, columnas) %}
select {{ columnas | join(', ') }}, count(*) as repeticiones
from {{ model }}
group by {{ columnas | join(', ') }}
having count(*) > 1
{% endtest %}
