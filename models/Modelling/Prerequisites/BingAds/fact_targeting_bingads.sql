select 
brand,
{{store_name('store')}},
cast(a.campaignId as string) campaign_id,
campaignName as campaign_name,
adGroupId as adgroup_id,
AdgroupName as adgroup_name,
AdId as ad_id,
date(timeperiod) as date,
coalesce(searchquery,'') search_term,
exchange_currency_rate,
exchange_currency_code, 
'Shopify' as platform_name,
'Bing' as ad_channel,
'Bing' as campaign_type, 
'Keyword' as targeting_type,
coalesce(cast(keywordId as string),'') targeting_id,
sum(cast(null as numeric)) bid_amount,
sum(cast(clicks as int)) clicks,
sum(cast(impressions as int)) impressions,
sum(cast(conversions as int)) conversions,
sum(cast(conversions as int)) quantity,
sum(cast(spend as numeric)) as spend,
sum(cast(revenue as numeric)) as sales   
from {{ ref('BingSearchQueryPerformanceReport')}} a
left join (select distinct campaignId, exchange_currency_code, exchange_currency_rate from {{ ref('BingAdPerformanceReport')}}) c
on a.campaignId = c.campaignId
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16