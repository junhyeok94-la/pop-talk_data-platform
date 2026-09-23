{% macro observation_recency_order() -%}
source_observed_at desc nulls last,
publication_revision desc,
publication_completed_at desc,
source_run_id desc,
artifact_version desc
{%- endmacro %}
