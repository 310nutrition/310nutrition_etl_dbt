select 
distinct
b.brand_name,
c.platform_name,
e.order_channel,
c.store_name,
product_id,
product_name,
sku,
subscription_id,
customer_id,
utm_source,
utm_medium,
created_at,
next_charge_scheduled_at,
order_interval_frequency,
order_interval_unit,
cancellation_reason,
cancelled_at,
order_day_of_month,
presentment_currency,
cancellation_reason_comments
from {{ ref('fact_order_lines')}} a
left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') b
on a.brand_key = b.brand_key 
left join {{ ref('dim_platform')}} c
on a.platform_key = c.platform_key 
left join (select product_key, product_id, product_name, sku from {{ref('dim_product')}} where status = 'Active') d
on a.product_key = d.product_key
left join (select * from {{ ref('dim_subscription')}} where status = 'Active') e
on a.subscription_key = e.subscription_key 


