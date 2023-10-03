Select 
brand_name,
c.platform_name,
store_name,
date,
coalesce(product_id,'') product_id,
coalesce(sku,'') sku,
--product_status,
product_name,
{% if var('ga_flag') %}
coalesce(event_name,'') event_name,
coalesce(source,'') source,
coalesce(medium,'') medium,
coalesce(campaign,'') campaign,
coalesce(keyword,'') keyword,
coalesce(content,'') content,
coalesce(landing_page_path,'') landing_page_path,
{% else %} 
'' as event_name,
'' as source,
'' as medium,
'' as campaign,
'' as keyword,
'' as content,
'' as landing_page_path,
{% endif %} 
avg(buy_box_percentage) buy_box_percentage,
sum(mobile_sessions) mobile_sessions,
sum(browser_sessions) browser_sessions,
sum(tablet_sessions) tablet_sessions,
sum(sessions) sessions,
sum(mobile_page_views) mobile_page_views,
sum(browser_page_views) browser_page_views,
sum(tablet_page_views) tablet_page_views,
sum(page_views) page_views,
sum(glance_views) glance_views
from {{ ref('fact_traffic') }} a
left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') b
on a.brand_key = b.brand_key
left join {{ ref('dim_platform') }} c
on a.platform_key = c.platform_key
left join (select product_key, product_id, product_name, sku from {{ref('dim_product')}} where status = 'Active') d
on a.product_key = d.product_key
{% if var('ga_flag') %}
left join {{ ref('dim_utm_channel') }} e
on a.utm_key = e.utm_key
left join {{ ref('dim_event') }} f
on a.event_key = f.event_key
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
{% else %} 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
{% endif %} 