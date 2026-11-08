{# Falla con las filas cuyo valor cae fuera de [minimo, maximo] (los nulos se ignoran). #}
{% test rango_fisico(model, column_name, minimo, maximo) %}
select {{ column_name }} as valor_fuera_de_rango
from {{ model }}
where {{ column_name }} < {{ minimo }} or {{ column_name }} > {{ maximo }}
{% endtest %}
