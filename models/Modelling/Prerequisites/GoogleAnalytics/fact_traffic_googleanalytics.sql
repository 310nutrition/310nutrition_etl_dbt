select
brand,
store_name,
platform_name,
product_id,
sku,
event_name,
source,
medium,
campaign,
keyword,
content,
landing_page_path,
date,
exchange_currency_rate,
exchange_currency_code,
sum(mobile_sessions) mobile_sessions,
sum(browser_sessions) browser_sessions,
sum(tablet_sessions) tablet_sessions,
sum(sessions) sessions,
sum(mobile_pageviews) mobile_pageviews,
sum(browser_pageviews) browser_pageviews,
sum(tablet_pageviews) tablet_pageviews,
sum(pageviews) pageviews,
-- applicable for amazon
sum(glance_views) glance_views,
avg(buybox_percentage) as buybox_percentage,
sum(quantity) as quantity,
sum(product_sales) as product_sales
from (
    select
    brand,
    store as store_name,
    platform_name,
    cast(null as string) as product_id,
    cast(null as string) as sku,
    a.event_name,
    source,
    medium,
    name as campaign,
    '' as keyword,
    '' as  content,
    string_value as landing_page_path,
    DATE(CONCAT(SUBSTRING(a.event_date, 1, 4), '-', SUBSTRING(a.event_date, 5, 2), '-', SUBSTRING(a.event_date, 7, 2))) date,
    case when category = 'mobile' and a.event_name = 'session_start' then 1 else 0 end as mobile_sessions,
    case when category = 'desktop' and a.event_name = 'session_start' then 1 else 0 end as browser_sessions,
    case when category = 'tablet' and a.event_name = 'session_start' then 1 else 0 end as tablet_sessions,
    case when a.event_name = 'session_start' then 1 else 0 end as sessions,
    case when category = 'mobile' and a.event_name = 'page_view' then 1 else 0 end as mobile_pageviews,
    case when category = 'desktop' and a.event_name = 'page_view' then 1 else 0 end as browser_pageviews,
    case when category = 'tablet' and a.event_name = 'page_view' then 1 else 0 end as tablet_pageviews,
    case when a.event_name = 'page_view' then 1 else 0 end as pageviews,  
    cast(null as numeric) as glance_views, 
    cast(null as numeric) as buybox_percentage,
    cast(total_item_quantity as int) as quantity,
    {% if var('currency_conversion_flag') %}
    cast(purchase_revenue_in_usd as numeric) as product_sales, 
    {% else %}
    cast(purchase_revenue as numeric) as product_sales,
    {% endif %}
    1 as exchange_currency_rate,
    '' as exchange_currency_code
    from {{ref('GoogleAnalyticsEventsEventParams')}} a
    where key = 'page_location'
    ) 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15