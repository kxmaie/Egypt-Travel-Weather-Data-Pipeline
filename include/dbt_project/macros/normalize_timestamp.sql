{% macro normalize_timestamp_ntz(expression) -%}
    COALESCE(
        TRY_CAST(CAST({{ expression }} AS VARCHAR) AS TIMESTAMP_NTZ),
        TO_TIMESTAMP_NTZ(
            CAST(
                TRY_TO_DOUBLE(CAST({{ expression }} AS VARCHAR)) / 1000000000
                AS BIGINT
            )
        )
    )
{%- endmacro %}
