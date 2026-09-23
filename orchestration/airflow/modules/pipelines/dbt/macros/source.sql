{% macro source(source_name, table_name) %}
    {# builtin 호출을 유지해 manifest에 원래 source 의존성을 기록한다. #}
    {% set original = builtins.source(source_name, table_name) %}
    {% set bindings = var('pop_talk_relations', {}) %}
    {% set source_id = 'source.' ~ project_name ~ '.' ~ source_name ~ '.' ~ table_name %}
    {% set bound = bindings.get(source_id) %}
    {% if bound %}
        {{ return(api.Relation.create(database=bound['database'], schema=bound['schema'], identifier=bound['identifier'])) }}
    {% elif bindings and execute %}
        {{ exceptions.raise_compiler_error('Missing source snapshot binding: ' ~ source_id) }}
    {% else %}
        {{ return(original) }}
    {% endif %}
{% endmacro %}
