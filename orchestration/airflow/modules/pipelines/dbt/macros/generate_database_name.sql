{% macro generate_database_name(custom_database_name=none, node=none) -%}
    {%- set bound = var('pop_talk_relations', {}).get(node.unique_id) -%}
    {%- if bound -%}
        {{ bound['database'] }}
    {%- else -%}
        {{ custom_database_name | trim if custom_database_name else target.database }}
    {%- endif -%}
{%- endmacro %}
