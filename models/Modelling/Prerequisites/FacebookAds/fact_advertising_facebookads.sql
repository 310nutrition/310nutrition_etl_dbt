with parent as (
    select 
    brand,
    store,
    campaign_id,
    campaign_name,
    adset_id, 
    adset_name, 
    ad_id,
    date(date_start) as date,
    exchange_currency_rate,
    exchange_currency_code,
    sum(clicks) clicks,
    sum(cast(impressions as numeric)) as impressions,
    cast(null as numeric) quantity,
    sum(spend) spend
    from {{ref('FacebookAdinsights')}}
    group by 1,2,3,4,5,6,7,8,9,10
    ),

child1 as (
    select 
    brand,
    store,
    campaign_id,
    campaign_name,
    adset_id, 
    adset_name, 
    ad_id,
    date(date_start) as date,
    sum(cast(action_values_value as numeric)) as sales
    from {{ref('FacebookAdinsightsActionValues')}}
    where action_values_action_type = 'offsite_conversion.fb_pixel_purchase'
    group by 1,2,3,4,5,6,7,8
    ),

child2 as (
    select 
    brand,
    store,
    campaign_id,
    campaign_name,
    adset_id, 
    adset_name, 
    ad_id,
    date(date_start) as date,
    sum(coalesce(cast(j.value as numeric),0)) as conversions
    from {{ref('FacebookAdinsights')}}
    left join unnest(actions) j 
    where j.action_type = 'purchase'
    group by 1,2,3,4,5,6,7,8
    )

select 
a.brand,
{{ store_name('a.store') }},
a.campaign_id,
cast(null as string) as flow_id,
a.adset_id as adgroup_id, 
a.ad_id,
cast(null as string) as product_id,
cast(null as string) as sku,
a.date,
exchange_currency_rate,
exchange_currency_code,
'Shopify' as platform_name,
'Facebook' as ad_channel,
'Facebook' as campaign_type,
'Facebook' as ad_type,
sum(clicks) as clicks,
sum(impressions) as impressions,
sum(quantity) quantity,
sum(spend) as spend,
sum(sales) as sales,
sum(conversions) as conversions,
sum(cast(null as numeric)) email_deliveries,
sum(cast(null as numeric)) email_opens,
sum(cast(null as numeric)) email_unsubscriptions
from parent a left join child1 b
on a.brand  = b.brand and a.store = b.store and a.campaign_id = b.campaign_id and a.adset_id = b.adset_id and a.adset_name = b.adset_name and a.ad_id = b.ad_id and a.date = b.date 
left join child2 c
on a.brand  = c.brand and a.store = c.store and a.campaign_id = c.campaign_id and a.adset_id = c.adset_id and a.adset_name = c.adset_name and a.ad_id = c.ad_id and a.date = c.date 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
