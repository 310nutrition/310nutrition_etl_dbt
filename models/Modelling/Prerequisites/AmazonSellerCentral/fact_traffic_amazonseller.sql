
select 
brand,
{{ store_name('marketplaceName') }},
'Amazon Seller Central' as platform_name,
childASIN as product_id,
cast(null as string) as sku,
cast(null as string) event_name,
'' as source,
'' as medium,
'' as campaign,
'' as keyword,
'' as content,
cast(null as string) as landing_page_path,
date,
exchange_currency_rate,
exchange_currency_code,
sum(mobileAppSessions) as mobile_sessions,
sum(browserSessions) as browser_sessions,
cast(null as int) tablet_sessions,
sum(sessions) as sessions,
sum(mobileAppPageViews) as mobile_pageviews,
sum(browserPageViews) as browser_pageviews,
cast(null as int) tablet_pageviews,
sum(pageViews) as pageviews,   
sum(cast(null as numeric)) as glance_views, 
avg(buyBoxPercentage) as buybox_percentage,
sum(unitsOrdered) as quantity,
sum(orderedProductSales_amount) as product_sales
from {{ ref('SalesAndTrafficReportByChildASIN') }} 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15