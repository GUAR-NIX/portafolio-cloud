{# Falla con las filas donde la expresión booleana es falsa. #}
{% test cumple_expresion(model, expresion) %}
select * from {{ model }} where not ({{ expresion }})
{% endtest %}
