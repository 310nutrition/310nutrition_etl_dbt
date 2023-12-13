{{config(materialized="table")}}


select
    platform_name,
    store_name,
    date,
    Custom_Grouping,
    sum(sessions) sessions,
    round(sum(sessions*sessionConversionRate),0) converted_sessions,
    round(sum(purchaseRevenue),2) as purchaseRevenue,
    sum(transactions) as transactions
from {{ref('ga4_channel_session')}} a
left join {{ref('custom_channel_mapping')}} b
on a.sessionSourceMedium = b.sessionSourceMedium
group by 1,2,3,4
order by 3 desc, 4