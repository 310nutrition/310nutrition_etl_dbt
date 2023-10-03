{{config(materialized="table")}}

with marketing as (
    select
        brand_name,
        platform_name,
        store_name,
        date,
        case
            when ad_channel = 'Klaviyo' then 'Klaviyo'
            else CONCAT(ad_channel, ' Ads')
            end as ad_platform,
        sum(adspend) as adspend
    from {{ref('marketing_deepdive')}}
    where platform_name = 'Shopify'
    group by 1,2,3,4,5
),

traffic_date as (
    select 
        brand as brand_name,
        platform_name,
        {{store_name('store_name')}},
        date,
        Mapped_source,
        sum(totalUsers) as totalUsers,
        sum(activeUsers) as activeUsers,
        sum(sessions) as sessions,
        sum(engagedSessions) as engagedSessions
    from {{ref('ga4_traffic_sourcemedium')}}
    group by 1,2,3,4,5
)

select
    a.platform_name,
    a.store_name,
    a.date,
    a.attribution_type,
    a.gross_sales,
    coalesce(b.adspend,0) as adspend,
    coalesce(c.totalUsers,0) as totalUsers,
    coalesce(c.activeUsers,0) as activeUsers,
    coalesce(c.sessions,0) as sessions,
    coalesce(c.engagedSessions,0) as engagedSessions
from {{ref('sales_by_sourcemedium')}} a
left join marketing b
    on a.brand_name = b.brand_name and a.platform_name = b.platform_name and a.store_name = b.store_name and a.date=b.date and a.attribution_type = b.ad_platform
left join traffic_date c
    on a.brand_name = c.brand_name and a.platform_name = c.platform_name and a.store_name = c.store_name and a.date=c.date and a.attribution_type = c.Mapped_source
order by adspend desc