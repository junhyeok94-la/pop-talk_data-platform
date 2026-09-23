{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set bound = var('pop_talk_relations', {}).get(node.unique_id) -%}
    {%- if bound -%}
        {{ bound['schema'] }}
    {%- elif custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
