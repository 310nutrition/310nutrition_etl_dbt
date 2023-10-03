{{config(materialized="table")}}

{% set table_name_query %}
{{set_table_name('%ga4%sales_attribution%')}}    
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

    {# Check if 'purchaserevenue_in' column exists in the current table i #}
    {% if i.split('.')[2].split('_')[var('storename_position_in_tablename')] == 'US' %}
        {% set purchaserevenue_column = True %}
    {% else %}
        {% set purchaserevenue_column = False %}
    {% endif %}
    
    
    Select * {{exclude()}} (row_num)
    FROM (select
        '310 Nutrition' as brand,
        'Shopify' as platform_name,
        '{{store}}' as store_name,
        dimensionHeaders,
        metricHeaders,
        start_date,
        end_date,
        trim(campaignName) as campaignName,
        trim(cast(campaignId as string)) as campaignId,
        trim(cast(transactionId as string)) as transactionId,
        trim(SPLIT(sourceMedium, ' / ')[SAFE_OFFSET(0)]) AS source,
        trim(SPLIT(sourceMedium, ' / ')[SAFE_OFFSET(1)]) AS medium,
        sourceMedium,
        {% if purchaserevenue_column == True %}
            round(coalesce(purchaseRevenue,cast(purchaseRevenue_in as NUMERIC)),2) as purchaseRevenue,
        {% else %}
            round(purchaseRevenue,2) as purchaseRevenue,
        {% endif %}
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        dense_rank() over(partition by transactionId,campaignid,campaignname,sourceMedium order by _daton_batch_runtime desc) as row_num
    from {{i}} a
    where transactionId is not null and length(cast(transactionId as string)) in (8,13))
    where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}