{% if target.type =='bigquery' %}

select * {{exclude()}} (row_num) from 
    (
    select 
    source,
    medium,
    name as campaign,
    '' as keyword,
    '' as  content,
    'Google Analytics' as channel_name, 
    DATE(CONCAT(SUBSTRING(event_date, 1, 4), '-', SUBSTRING(event_date, 5, 2), '-', SUBSTRING(event_date, 7, 2))) last_updated_date,
    row_number() over(partition by source, medium, name 
    order by DATE(CONCAT(SUBSTRING(event_date, 1, 4), '-', SUBSTRING(event_date, 5, 2), '-', SUBSTRING(event_date, 7, 2))) desc) row_num
    from {{ ref('GoogleAnalyticsEvents') }}
    )
where row_num = 1

    
{% else %}

select
null as source,
null as medium,
null as campaign,
null as keyword,
null as content,
null as channel_name, 
null as last_updated   

{% endif %}
