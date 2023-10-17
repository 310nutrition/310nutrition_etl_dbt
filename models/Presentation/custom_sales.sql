with custom_sales as (select 
e.brand_name,
c.platform_name,
c.store_name,
f.order_id,
a.date,
b.email,
a.customer_id,
a.transaction_type as order_type,
a.product_id,
a.sku,
i.product_name,
product_category,
mrp,
category,
sub_category, 
cogs,
quantity,
currency_code as currency,
case when a.transaction_type='Order' or a.transaction_type='Cancelled' then item_total_price else 0 end as item_total_price,
case when a.transaction_type='Order' or a.transaction_type='Cancelled' then item_subtotal_price else 0 end as item_subtotal_price  ,
case when a.transaction_type='Order' or a.transaction_type='Cancelled' then item_total_tax else 0 end as item_taxes,
case when a.transaction_type='Order' or a.transaction_type='Cancelled' then item_shipping_price else 0 end as item_shipping,
cast(0 as numeric) as item_shipping_tax,
case when a.transaction_type='Order' or a.transaction_type='Cancelled' then item_shipping_discount else 0 end as item_shipping_discount,
case when a.transaction_type='Order' or a.transaction_type='Cancelled' then item_giftwrap_price else 0 end as item_giftwrap,
case when a.transaction_type='Order' or a.transaction_type='Cancelled' then item_discount else 0 end as item_discount,
case when a.transaction_type='Return' then item_subtotal_price else 0 end AS item_refund_amount,
case when c.platform_name like 'Shopify' then
    case when d.subscription_id is not null then True ELSE False END 
    else
    case when a.subscription_id is not null then True ELSE False END
    end as is_subscription,
case when c.platform_name like 'Shopify' then d.subscription_id else a.subscription_id end as subscription_id,
d.external_product_id as subscription_product_id,
d.customer_id as subscription_customer_id,
CASE WHEN a.customer_id is null then 1 else DENSE_RANK() OVER (PARTITION BY a.customer_id ORDER BY a.date, a.order_id) end AS customer_order_sequence,
acquisition_date as customer_first_order_date,
last_order_date as customer_last_order_date,
ship_city as delivery_city,
ship_state as delivery_state,
ship_country as delivery_country
from {{ref('fact_order_lines')}} a left join {{ref('dim_customer')}} b on a.customer_key=b.customer_key
left join {{ref('dim_platform')}} c on a.platform_key=c.platform_key
left join {{ref('dim_orders')}} f on a.order_key = f.order_key
left join (select * from {{ref('dim_subscription')}} where effective_end_date = '9999-12-31') d
on a.subscription_key = d.subscription_key
left join {{ref('dim_brand')}} e on a.brand_key = e.brand_key
left join (select product_key, product_id, product_name, sku,product_category,mrp,category,sub_category, cogs,start_date,end_date from {{ref('dim_product')}} ) i
on a.product_key = i.product_key
),

sales as (select * 
from custom_sales 
where lower(order_type) = 'order'), --added by akash

marketing as (SELECT brand_name, platform_name ,store_name, date, sum(adspend) as adspend 
FROM {{ref('marketing_deepdive')}}
group by 1,2,3,4) --added by akash

select a.*, coalesce(b.adspend/count(*) over(partition by a.brand_name, a.platform_name ,a.store_name, a.date),0) as adspend
from sales as a
left join marketing as b
on a.brand_name = b.brand_name and a.platform_name = b.platform_name and a.store_name = b.store_name and a.date = b.date
union all
select *, 0 as adspend from custom_sales where lower(order_type) != 'order'--added by Akash