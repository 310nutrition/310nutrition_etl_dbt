{{config(materialized="table")}}

{% set table_name_query %}
{{set_table_name('%sales_sourcemedium%')}}    
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
        '310 Nutrition' as brand,
        'Shopify' as platform_name,
        '{{store}}' as store_name,
        date,
        start_date,
        end_date,
        transactionId,
        trim(SPLIT(sourceMedium, ' / ')[SAFE_OFFSET(0)]) AS source,
        trim(SPLIT(sourceMedium, ' / ')[SAFE_OFFSET(1)]) AS medium,
        sourceMedium,
        case
            when lower(sourceMedium) like '%organic%' or lower(sourceMedium) like '%search%' and lower(sourceMedium) not like '%reviewology%organic%' then 'Organic'
            when lower(sourceMedium) like '%email%' and lower(sourceMedium) not like '%refer%' then 'Email'
            when lower(sourceMedium) like '%sms%' then 'sms'
            when lower(sourceMedium) like '%referral%' or lower(sourceMedium) like '%social%' or lower(sourceMedium) like '%influencer%' or lower(sourceMedium) like '%refer%'  then 'Referral'
            when lower(sourceMedium) like '%direct%' then 'Direct'
            when lower(sourceMedium) like '%bing%paid%' or lower(sourceMedium) like '%bing%cpc%' then 'Bing Ads'
            when lower(sourceMedium) like '%fb%paid%' or lower(sourceMedium) like '%fb%cpc%' or lower(sourceMedium) like '%facebook%cpc%' or lower(sourceMedium) like '%facebook%paid%' or lower(sourceMedium) like '%ig%cpc%' or lower(sourceMedium) like '%ig%cpc%' or lower(sourceMedium) like '%instagram%cpc%' or lower(sourceMedium) like '%instagram%paid%' then 'Facebook Ads'
            when lower(sourceMedium) like '%google%paid%' or lower(sourceMedium) like '%google%cpc%' or lower(sourceMedium) like '%reviewology%' then 'Google Ads'
            when lower(sourceMedium) like '%pinterest%paid%' or lower(sourceMedium) like '%pinterest%cpc%' then 'Pinterest Ads'
            else 'Unattributed' end as Mapped_source,
        round(coalesce(purchaseRevenue,cast(purchaseRevenue_in as NUMERIC)),2) as purchaseRevenue,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        dense_rank() over(partition by date,transactionId,sourceMedium order by _daton_batch_runtime desc) as row_num
    from {{i}} a
    where transactionId is not null and length(cast(transactionId as string)) in (8,13))
    where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}