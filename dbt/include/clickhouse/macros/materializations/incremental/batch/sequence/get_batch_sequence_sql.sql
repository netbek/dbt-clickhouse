{% macro clickhouse__get_batch_sequence_sql(sql, filter, column, batch_size, range_min, range_max, offset, relation_alias=none) -%}
  {%- set col %}{% if relation_alias %}{{ relation_alias }}.{% endif %}{{ adapter.quote(column) }}{% endset -%}
  {%- set filter_sql -%}
    where (
      {{ col }} >= {{ range_min }} + ({{ batch_size }} * {{ offset }}) and
      {{ col }} <  {{ range_min }} + ({{ batch_size }} * {{ offset + 1 }}) and
      {{ col }} <= {{ range_max }}
    )
  {%- endset -%}
  {%- set filtered_sql = sql | replace(filter, filter_sql) -%}
  {{ return(filtered_sql) }}
{%- endmacro %}
