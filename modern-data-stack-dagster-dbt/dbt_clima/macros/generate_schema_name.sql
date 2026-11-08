{# Usa el esquema de la capa tal cual (staging, intermediate, marts) en vez de main_staging. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}
