{#- Имена схем берём как есть (silver, gold), без префикса target.schema -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}{{ target.schema }}{%- else -%}{{ custom_schema_name | trim }}{%- endif -%}
{%- endmacro %}


{#- Строка → timestamp. Iceberg хранит микросекунды, поэтому в Trino timestamp(6) -#}
{% macro to_ts(column) -%}
    {%- if target.type == 'trino' -%}
        try_cast({{ column }} as timestamp(6))
    {%- else -%}
        try_cast({{ column }} as timestamp)
    {%- endif -%}
{%- endmacro %}


{% macro to_money(column) -%}
    try_cast({{ column }} as decimal(12, 2))
{%- endmacro %}


{#- Trino + Iceberg умеют MERGE; в DuckDB (CI) используем delete+insert -#}
{% macro merge_strategy() -%}
    {{ return('merge' if target.type == 'trino' else 'delete+insert') }}
{%- endmacro %}


{#- Дата, начиная с которой инкрементальная модель перечитывает источник -#}
{% macro incremental_since(column) -%}
    (select coalesce(max({{ column }}), date '1900-01-01') from {{ this }})
{%- endmacro %}
