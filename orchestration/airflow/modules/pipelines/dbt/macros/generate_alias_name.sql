{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- set bound = var('pop_talk_relations', {}).get(node.unique_id) -%}
    {%- if bound -%}
        {{ bound['identifier'] }}
    {%- elif custom_alias_name -%}
        {{ custom_alias_name | trim }}
    {%- else -%}
        {{ node.name }}
    {%- endif -%}
{%- endmacro %}
