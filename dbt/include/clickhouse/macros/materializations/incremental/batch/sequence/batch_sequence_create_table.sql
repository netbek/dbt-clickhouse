{% macro clickhouse__batch_sequence_create_table(temporary, relation, sql) %}
  {%- set batch_filter = config.get('batch_filter', '/* __BATCH_FILTER__ */') -%}
  {%- set batch_size = config.get('batch_size', 100000) | int -%}
  {%- set batch_table = config.require('batch_table') -%}
  {%- set batch_column = config.require('batch_column') -%}

  {%- set batch_relation = adapter.get_relation(this.database, this.schema, batch_table) -%}
  {%- set min_max = get_min_max(batch_relation, batch_column) | as_native -%}
  {%- set range_min = min_max['min'][0] | int -%}
  {%- set range_max = min_max['max'][0] | int -%}

  {%- if sql.find(batch_filter) == -1 -%}
    {%- set error_message -%}
      Model '{{ model.unique_id }}' does not include the required string '{{ batch_filter }}' in its sql
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
  {%- endif -%}

  {%- set boundaries = clickhouse__get_batch_sequence_boundaries(batch_size, range_min, range_max) | as_native -%}
  {%- set range_min = boundaries['range_min'][0] | int -%}
  {%- set range_max = boundaries['range_max'][0] | int -%}
  {%- set num_batches = boundaries['num_batches'][0] | int -%}

  -- commit each batch as a separate transaction
  {% for offset in range(num_batches) -%}
    {%- set msg = "Loading batch " ~ (offset + 1) ~ " of " ~ (num_batches) -%}
    {{ print(msg) }}

    {%- set filtered_sql = clickhouse__get_batch_sequence_sql(sql, batch_filter, batch_column, batch_size, range_min, range_max, offset) -%}

    {% call statement('main') %}
      {% if offset == 0 %}
        {{ get_create_table_as_sql(temporary, relation, filtered_sql) }}
      {% else %}
        {{ clickhouse__insert_into(relation, filtered_sql) }}
      {% endif %}
    {% endcall %}
  {% endfor %}
{% endmacro %}
