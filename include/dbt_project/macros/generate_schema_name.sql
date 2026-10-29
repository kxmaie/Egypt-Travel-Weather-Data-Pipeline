{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set schema_prefix = env_var('DBT_SCHEMA_PREFIX', '') | trim -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif schema_prefix -%}
        {{ schema_prefix }}_{{ custom_schema_name | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
