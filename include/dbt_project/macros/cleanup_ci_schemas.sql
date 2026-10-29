{% macro cleanup_ci_schemas() %}
    {% set prefix = env_var('DBT_SCHEMA_PREFIX', '') | trim %}
    {% if prefix[:3] != 'CI_' %}
        {{ exceptions.raise_compiler_error('Refusing to clean schemas without a CI_ prefix') }}
    {% endif %}
    {% if target.schema[:3] != 'CI_' %}
        {{ exceptions.raise_compiler_error('Refusing cleanup because the active target schema is not isolated') }}
    {% endif %}
    {% for schema_name in [target.schema, prefix ~ '_silver_layer', prefix ~ '_gold_layer'] %}
        {% do run_query('DROP SCHEMA IF EXISTS ' ~ adapter.quote(schema_name)) %}
    {% endfor %}
{% endmacro %}
