{% macro ref(model_name) %}
    {# 이 프로젝트는 단일 package의 비버전 모델만 사용한다. #}
    {% set original = builtins.ref(model_name) %}
    {% set bindings = var('pop_talk_relations', {}) %}
    {% set model_id = 'model.' ~ project_name ~ '.' ~ model_name %}
    {% if bindings and execute and model_id not in bindings %}
        {{ exceptions.raise_compiler_error('Missing model result binding: ' ~ model_id) }}
    {% endif %}
    {{ return(original) }}
{% endmacro %}
