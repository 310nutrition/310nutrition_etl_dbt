{{config(materialized="table")}}

{% set table_name_query %}
{{set_table_name('%channel_session_source_maping%')}}    
{% endset %}  

{% set results = run_query(table_name_query) %}
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
{% set results_list = [] %}
{% set tables_lowercase_list = [] %}
{% endif %}

{% for i in results_list %}
    {% if var('get_storename_from_tablename_flag') %}
        {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    Select * {{exclude()}} (row_num)
    FROM (select
        sessionSourceMedium,
        Custom_Grouping,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        dense_rank() over(partition by sessionSourceMedium order by _daton_batch_runtime desc) as row_num
    from {{i}} a)
    where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}