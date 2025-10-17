{# Same logic as is_incremental() but accept a graph model node #}
{% macro is_node_incremental(node) %}
    {#-- do not run introspective queries in parsing #}
    {% if not execute %}
        {{ return(False) }}
    {% else %}
        {% set relation = adapter.get_relation(node.database, node.schema, node.name) %}
        {{ return(relation is not none
                  and relation.type == 'table'
                  and (node.config.materialized == 'incremental' or node.config.materialized == 'distributed_incremental' )
                  and not should_full_refresh()) }}
    {% endif %}
{% endmacro %}
