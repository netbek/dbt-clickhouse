{% macro get_min_max(relation, column) -%}
  {% set sql %}
    select
      min({{ column }}) as min,
      max({{ column }}) as max
    from {{ relation }}
  {% endset %}
  {{ return(dbt_utils.get_query_results_as_dict(sql)) }}
{%- endmacro %}
