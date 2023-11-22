{{config(materialized="table")}}

{% set table_name_query %}
{{set_table_name('%googleanalytics4%traffic_conversions%')}}    
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
    from (
        select date, 
        sessionDefaultChannelGroup, 
        totalUsers, 
        sessions, 
        coalesce(sessionConversionRate, sessionConversionRate_in) as sessionConversionRate,
        dense_rank() over(partition by date, sessionDefaultChannelGroup order by _daton_batch_runtime desc) as row_num
        from {{i}}
    ) where row_num = 1
     {% if not loop.last %} union all {% endif %}
{% endfor %}