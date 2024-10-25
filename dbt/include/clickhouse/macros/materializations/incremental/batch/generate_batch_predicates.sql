{% macro generate_batch_predicates() %}
  {%- set materialized = config.get('materialized') -%}
  {%- set batch_type = config.get('batch_type') -%}

  {% if materialized == 'incremental' and batch_type == 'sequence' %}
    __BATCH_PREDICATES__
  {% endif %}
{% endmacro %}
