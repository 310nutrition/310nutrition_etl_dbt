select 
brand_name,
c.platform_name,
store_name,
order_channel,
date,
currency_code,
sum(quantity) quantity,
sum(total_price) total_price,
sum(subtotal_price) subtotal_price,
sum(shipping_price) shipping_price,
sum(giftwrap_price) giftwrap_price,
sum(order_discount) order_discount,
sum(shipping_discount) shipping_discount,
sum(total_tax) total_taxes
from {{ ref('fact_orders') }} a
left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') b
on a.brand_key = b.brand_key
left join {{ ref('dim_platform') }} c
on a.platform_key = c.platform_key
left join {{ ref('dim_orders') }} d
on a.order_key = d.order_key
where transaction_type = 'Order' and is_cancelled = false
group by 1,2,3,4,5,6
