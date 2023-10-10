{% if var('customer') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

select
email,
c.order_id,
b.customer_id,
{% if var('ga_flag') %}
case when g.subscription_id is not null then 'Subscriber' else 'Non-Subscriber' end as customer_type, 
{% endif %} 
date,
acquisition_date,
--substr(cast(acquisition_date as STRING),1,7) as acquisition_month,
--substr(cast(date as STRING),1,7) as order_month,
date_trunc(acquisition_date,month) as acquisition_month, --added by akash because string date was throwing error in tableau
date_trunc(date,month) as order_month, --added by akash because string date was throwing error in tableau
last_order_date,
brand_name,
e.platform_name,
e.store_name,
f.product_id,
product_name,
c.source,
c.medium,
c.campaign,
f.sku,
currency_code,
sum(item_total_price) as item_total_price ,
sum(item_subtotal_price) as item_subtotal_price,
sum(quantity) as quantity,
dense_rank() over(partition by b.customer_id,e.platform_name,e.store_name,is_cancelled order by date, order_id) as order_number --added by Akash
from {{ ref('fact_order_lines')}} a
left join {{ ref('dim_customer')}} b
on a.customer_key = b.customer_key
left join {{ ref('dim_orders')}} c
on a.order_key = c.order_key
left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') d
on a.brand_key = d.brand_key
left join {{ ref('dim_platform')}} e
on a.platform_key = e.platform_key
left join (select product_key, product_id, product_name, sku from {{ref('dim_product')}} where status = 'Active') f
on a.product_key = f.product_key
{% if var('ga_flag') %}
left join {{ ref('dim_subscription')}} g
on a.subscription_key = g.subscription_key 
{% endif %} 
where email is not null and transaction_type = 'Order'
{% if var('ga_flag') %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
{% else %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
{% endif %} 