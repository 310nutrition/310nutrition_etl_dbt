select 
brand,
{{store_name('store')}},
cast(campaign_id as string) campaign_id,
campaign_name,
cast(null as string) as flow_id,
cast(null as string) as flow_name,
cast(ad_group_id as string) as adgroup_id, 
ad_group_name as adgroup_name, 
cast(null as string) as ad_id,
cast(product_item_id as string) as product_id,
cast(null as string) as sku,
date(date) as date,
exchange_currency_rate,
exchange_currency_code,
'Shopify' as platform_name,
campaign_advertising_channel_type as ad_channel,
'Google' as campaign_type,
cast(null as string) as flow_type,
sum(cast(clicks as numeric)) clicks,
sum(cast(impressions as numeric)) impressions,
sum(conversions) conversions,
sum(cast(null as numeric)) as quantity,
sum(round((cast(cost_micros as numeric)/1000000),2)) as spend,
sum(conversions_value) as sales,
sum(cast(null as numeric)) email_deliveries,
sum(cast(null as numeric)) email_opens,
sum(cast(null as numeric)) email_unsubscriptions
from {{ ref('GoogleAdsShoppingPerformanceView') }}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18

union all

select 
brand,
{{store_name('store')}},
cast(campaign_id as string) campaign_id,
campaign_name,
cast(null as string) as flow_id,
cast(null as string) as flow_name,
cast(null as string) as adgroup_id, 
cast(null as string) as adgroup_name, 
cast(null as string) as ad_id,
cast(null as string) as product_id,
cast(null as string) as sku,
date,
exchange_currency_rate,
exchange_currency_code,
'Shopify' as platform_name,
'Google' as ad_channel,
campaign_advertising_channel_type as campaign_type,
cast(null as string) as flow_type,
sum(cast(clicks as numeric)) clicks,
sum(cast(impressions as numeric)) impressions,
sum(conversions) conversions,
sum(cast(null as numeric)) as quantity,
sum(round((cast(cost_micros as numeric)/1000000),2)) as spend,
sum(conversions_value) as sales,
sum(cast(null as numeric)) email_deliveries,
sum(cast(null as numeric)) email_opens,
sum(cast(null as numeric)) email_unsubscriptions
from {{ ref('GoogleAdsCampaign') }}
where campaign_advertising_channel_type != 'SHOPPING'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18