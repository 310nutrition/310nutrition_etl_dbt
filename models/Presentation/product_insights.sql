{% if var('product_insights') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}


with sales as (
    select 
    brand_key,
    platform_key,
    product_key,
    date,
    sum(item_total_price) item_total_price,
    sum(quantity) quantity 
    from {{ ref('fact_order_lines')}}
    where transaction_type = 'Order'
    group by 1,2,3,4
),

returns as (
    select 
    brand_key,
    platform_key,
    product_key,
    date,
    sum(quantity) units_returned
    from {{ ref('fact_order_lines')}}
    where transaction_type = 'Return'
    group by 1,2,3,4
)

Select 
b.brand_name,
c.platform_name,
c.store_name,
a.date,
product_id,
mrp, 
category, 
sub_category, 
cogs, 
product_category,
sum(item_total_price) item_revenue,
sum(quantity) units_sold,
sum(units_returned) units_returned
from {{ ref('fact_inventory')}} a 
left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') b
on a.brand_key = b.brand_key
left join {{ ref('dim_platform')}} c
on a.platform_key = c.platform_key
left join (select product_key, product_id, product_name, sku, product_category,mrp,category,sub_category, cogs from {{ref('dim_product')}} where status = 'Active') d
on a.product_key = d.product_key
left join sales e
on a.platform_key = e.platform_key and a.brand_key = e.brand_key and a.date = e.date 
and a.product_key = e.product_key
left join returns f
on a.platform_key = f.platform_key and a.brand_key = f.brand_key and a.date = f.date 
and a.product_key = f.product_key
group by 1,2,3,4,5,6,7,8,9,10