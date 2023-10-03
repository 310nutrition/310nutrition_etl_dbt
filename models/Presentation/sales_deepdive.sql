with orders as (
    select 
    brand_key,
    platform_key,
    product_key,
    date,
    count(distinct order_key) orders,
    sum(quantity) quantity,
    sum(item_total_price) item_total_price,
    sum(item_subtotal_price) item_subtotal_price,
    sum(item_shipping_price) item_shipping_price,
    sum(item_giftwrap_price) item_giftwrap_price,
    sum(item_discount) item_discount,
    sum(item_shipping_discount) item_shipping_discount,
    sum(item_total_tax) item_total_tax
    from {{ ref('fact_order_lines')}} a
    where transaction_type = 'Order'
    group by 1,2,3,4
),

refunds_return_date as (
    select 
    brand_key,
    platform_key,
    product_key,
    date,
    sum(item_total_price) refunded_amount_by_return_date,
    sum(quantity) refunded_quantity_by_return_date
    from {{ ref('fact_order_lines') }}
    where transaction_type = 'Return'
    group by 1,2,3,4
),

refunds_order_date as (
    select 
    brand_key,
    platform_key,
    product_key,
    date as order_date,
    sum(refunded_amount_by_order_date) refunded_amount_by_order_date,
    sum(refunded_quantity_by_order_date) refunded_quantity_by_order_date
    from (
    select 
    a.brand_key,
    a.platform_key,
    a.date,
    a.product_key,
    case when (b.order_key is not null and b.product_key is not null) then b.item_total_price
    when (c.order_key is not null and c.product_key is not null) then c.item_total_price else null end as refunded_amount_by_order_date,
    case when (b.order_key is not null and b.product_key is not null) then b.quantity
    when (c.order_key is not null and c.product_key is not null) then c.quantity else null end as refunded_quantity_by_order_date,
    from {{ ref('fact_order_lines') }} a
    left join (
        select 
        order_key, 
        product_key, 
        sum(quantity) quantity , 
        sum(item_total_price) item_total_price 
        from {{ ref('fact_order_lines') }} 
        where transaction_type = 'Return' and platform_key not in (select distinct platform_key from {{ ref('dim_platform') }} where platform_name = 'Amazon Vendor Central') 
        group by 1,2) b
    on a.order_key = b.order_key and a.product_key = b.product_key 
    left join (
        select date, 
        order_key, 
        product_key, 
        sum(quantity) quantity , 
        sum(item_total_price) item_total_price 
        from {{ ref('fact_order_lines') }} 
        where transaction_type = 'Return' and platform_key in (select distinct platform_key from {{ ref('dim_platform') }} where platform_name = 'Amazon Vendor Central')
        group by 1,2,3) c
    on a.order_key = c.order_key and a.product_key = c.product_key and a.date = c.date
    where transaction_type = 'Order') 
    group by 1,2,3,4
),


traffic as (
    select 
    brand_key,
    platform_key,
    product_key,
    date,
    sum(sessions) sessions,
    sum(page_views) page_views,
    sum(glance_views) glance_views,
    sum(revenue) revenue,
    sum(sold_quantity) net_quantity
    from {{ ref('fact_traffic')}} 
    group by 1,2,3,4
),

advertising as (
    select 
    brand_key,
    platform_key,
    a.product_key,
    date,
    sum(sales) adsales,
    sum(spend) adspend,
    sum(clicks) clicks,
    sum(impressions) impressions,
    sum(conversions) marketing_conversions
    from {{ ref('fact_advertising')}} a
    left join {{ ref('dim_product')}} b
    on a.product_key = b.product_key
    where (b.product_id is not null and b.sku is not null)
    group by 1,2,3,4
)

select 
brand_name,
h.platform_name,
store_name,
product_id,
product_name,
f.product_key,
sku,
date,
orders,
quantity,
item_total_price,
item_subtotal_price,
item_shipping_price,
item_giftwrap_price,
item_discount,
item_shipping_discount,
item_total_tax,
refunded_quantity_by_order_date,
refunded_amount_by_order_date,
refunded_quantity_by_return_date,
refunded_amount_by_return_date,
(ifnull(item_subtotal_price,0) - ifnull(adsales,0)) organic_sales,
adsales,
adspend,
sessions,
page_views,
glance_views,
revenue,
net_quantity,
clicks,
impressions,
marketing_conversions   
from (
select 
coalesce(a.brand_key,b.brand_key,c.brand_key) brand_key,
coalesce(a.platform_key,b.platform_key,c.platform_key) platform_key,
coalesce(a.product_key,b.product_key,c.product_key) product_key,
coalesce(a.date,b.date,c.date) date,
orders,
quantity,
item_total_price,
item_subtotal_price,
item_shipping_price,
item_giftwrap_price,
item_discount,
item_shipping_discount,
item_total_tax,
refunded_quantity_by_order_date,
refunded_amount_by_order_date,
refunded_quantity_by_return_date,
refunded_amount_by_return_date,
adsales,
adspend,
sessions,
page_views,
glance_views,
revenue,
net_quantity,
clicks,
impressions,
marketing_conversions   
from orders a
full outer join advertising b
on a.brand_key = b.brand_key and a.platform_key = b.platform_key and a.product_key = b.product_key and a.date = b.date 
full outer join traffic c
on a.brand_key = c.brand_key and a.platform_key = c.platform_key and a.product_key = c.product_key and a.date = c.date
left join refunds_order_date d
on a.platform_key = d.platform_key and a.brand_key = d.brand_key and a.date = d.order_date and a.product_key = d.product_key
left join refunds_return_date e
on a.platform_key = e.platform_key and a.brand_key = e.brand_key and a.date = e.date and a.product_key = e.product_key
) f
left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') g
on f.brand_key = g.brand_key
left join {{ ref('dim_platform')}} h
on f.platform_key = h.platform_key
left join (select product_key, product_id, product_name, sku from {{ref('dim_product')}} where status = 'Active') i
on f.product_key = i.product_key