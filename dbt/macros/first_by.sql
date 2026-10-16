{#- Value at the earliest / latest ordering key. DuckDB spells it arg_min/arg_max, Trino min_by/max_by,
    so the same model runs on the CI target and on the lakehouse. -#}
{% macro first_by(value, key) -%}
  {{ 'arg_min' if target.type == 'duckdb' else 'min_by' }}({{ value }}, {{ key }})
{%- endmacro %}

{% macro last_by(value, key) -%}
  {{ 'arg_max' if target.type == 'duckdb' else 'max_by' }}({{ value }}, {{ key }})
{%- endmacro %}
