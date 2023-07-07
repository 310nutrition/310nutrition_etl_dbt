select 
date,
brand_name,
d.ad_channel,
d.campaign_id,
d.campaign_type,
c.platform_name,
store_name,
campaign_name,
adgroup_name,
search_term,
targeting_type,
targeting_text,
match_type,
currency_code,
sum(a.bid_amount) bid_amount,
sum(spend) adspend,
sum(sales) adsales,
sum(clicks) clicks,
sum(impressions) impressions,
sum(conversions) marketing_conversions
from {{ ref('fact_targeting')}} a
left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') b
on a.brand_key = b.brand_key
left join {{ ref('dim_platform')}} c
on a.platform_key = c.platform_key
left join {{ ref('dim_campaign')}} d
on a.campaign_key = d.campaign_key
left join {{ ref('dim_targeting')}} e
on a.targeting_key = e.targeting_key
left join {{ ref('dim_adgroup')}} f
on a.adgroup_key = f.adgroup_key
-- where c.status = 'Active'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
