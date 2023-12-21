{% if var('sales_overview') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

with orders as (
    select 
    brand_key,
    platform_key,
    date,
    currency_code,
    count(distinct order_key) orders,
    sum(quantity) quantity,
    sum(total_price) total_price,
    sum(subtotal_price) subtotal_price,
    sum(shipping_price) shipping_price,
    sum(giftwrap_price) giftwrap_price,
    sum(order_discount) order_discount,
    sum(shipping_discount) shipping_discount,
    sum(total_tax) total_taxes
    from {{ ref('fact_orders') }}
    where transaction_type = 'Order' 
    group by 1,2,3,4
),

refunds_return_date as (
    select 
    brand_key,
    platform_key,
    date,
    sum(total_price) refunded_amount_by_return_date,
    sum(quantity) refunded_quantity_by_return_date
    from {{ ref('fact_orders') }}
    where transaction_type = 'Return'
    group by 1,2,3
),

refunds_order_date as (
    select 
    brand_key,
    platform_key,
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
    group by 1,2,3
),

traffic as (
    select 
    brand_key,
    platform_key,
    date,
    sum(sessions) sessions,
    sum(page_views) page_views,
    sum(glance_views) glance_views,
    sum(revenue) revenue,
    sum(sold_quantity) net_quantity
    from {{ ref('fact_traffic') }}
    group by 1,2,3
),

advertising as (
    select 
    brand_key,
    platform_key,
    date,
    sum(sales) adsales,
    sum(spend) adspend,
    sum(impressions) impressions,
    sum(clicks) clicks
    from {{ ref('fact_advertising') }}
    group by 1,2,3
)

Select 
brand_name,
c.platform_name,
store_name,
a.date,
currency_code,
orders,
quantity,
total_price,
subtotal_price,
shipping_price,
giftwrap_price,
order_discount,
shipping_discount,
total_taxes,
refunded_quantity_by_return_date,
refunded_amount_by_return_date,
refunded_quantity_by_order_date,
refunded_amount_by_order_date,
(ifnull(subtotal_price,0) - ifnull(adsales,0)) organic_sales,
adsales,
adspend,
clicks,
impressions,
sessions,
page_views,
glance_views,
revenue,
net_quantity
from orders a
left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') b
on a.brand_key = b.brand_key
left join {{ ref('dim_platform') }} c
on a.platform_key = c.platform_key
left join refunds_order_date d
on a.platform_key = d.platform_key and a.brand_key = d.brand_key and a.date = d.order_date
left join refunds_return_date e
on a.platform_key = e.platform_key and a.brand_key = e.brand_key and a.date = e.date
full outer join traffic f   
on a.platform_key = f.platform_key and a.brand_key = f.brand_key and a.date = f.date
full outer join advertising g
on a.platform_key = g.platform_key and a.brand_key = g.brand_key and a.date = g.date