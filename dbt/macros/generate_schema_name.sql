{#
    dbt's default behavior concatenates the profile's target schema with
    any custom schema a model specifies (e.g. "public_staging"). This
    project's raw/staging/analytics schemas are already fixed names
    (postgres/init/01_create_schemas.sql, design.md section 15), so a
    model's custom schema should be used exactly as given instead.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
