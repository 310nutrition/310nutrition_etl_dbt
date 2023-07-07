Select 
b.brand_name,
c.platform_name,
c.store_name,
fulfillment_channel,
date,
product_id,
sku,
product_name,
event,
a.type,
value
from {{ ref('fact_inventory')}} a 
left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') b
on a.brand_key = b.brand_key
left join {{ ref('dim_platform')}} c
on a.platform_key = c.platform_key 
left join (select product_key, product_id, product_name, sku from {{ref('dim_product')}} where status = 'Active') d
on a.product_key = d.product_key
where product_id is not null