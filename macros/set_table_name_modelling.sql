{% macro set_table_name_modelling(variable) %}

    {% if target.type =='snowflake' %}
    select concat(table_catalog,'.',table_schema, '.',table_name) as tables,
    lower(concat(table_catalog,'.',table_schema, '.',table_name)) as tables_lowercase
    from INFORMATION_SCHEMA.TABLES 
    where lower(table_name) like '{{variable}}' and lower(table_schema)='{{ var("mdl_schema") }}' and lower(table_type) = 'view'
    {% else %}
    select concat(table_catalog,'.',table_schema, '.',table_name) as tables,
    lower(concat(table_catalog,'.',table_schema, '.',table_name)) as tables_lowercase
    from {{ var('mdl_database') }}.{{ var('mdl_schema') }}.INFORMATION_SCHEMA.TABLES
    where lower(table_name) like '{{variable}}' and lower(table_type) = 'view'
    {% endif %}

{% endmacro %}