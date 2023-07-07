with orders as (
    select 
    brand_key,
    platform_key,
    product_key,
    date,
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

traffic as (
    select 
    brand_key,
    platform_key,
    product_key,
    date,
    sum(sessions) sessions,
    sum(page_views) page_views,
    sum(sold_quantity) net_quantity
    from {{ ref('fact_traffic')}} 
    group by 1,2,3,4
),

advertising as (
    select 
    brand_key,
    platform_key,
    product_key,
    date,
    sum(sales) adsales,
    sum(spend) adspend
    from {{ ref('fact_advertising')}} 
    group by 1,2,3,4
),

aggregation as (
    select 
    brand_name,
    h.platform_name,
    store_name,
    product_id,
    product_name,
    sku,
    {% if target.type=='snowflake' %} 
    DATE_TRUNC(month,date) AS month_start_date,
    {% else %}
    DATE_TRUNC(date, month) month_start_date,
    {% endif %}
    sum(quantity) quantity,
    sum(net_quantity) net_quantity,
    sum(item_total_price) item_total_price,
    sum(item_subtotal_price) item_subtotal_price,
    sum(item_shipping_price) item_shipping_price,
    sum(item_giftwrap_price) item_giftwrap_price,
    sum(item_discount) item_discount,
    sum(item_shipping_discount) item_shipping_discount,
    sum(item_total_tax) item_total_tax,
    sum(organic_sales) organic_sales,
    sum(adsales) adsales,
    sum(adspend) adspend,
    sum(sessions) sessions,
    sum(page_views) page_views
    from (
    select 
    coalesce(a.brand_key,b.brand_key,c.brand_key) brand_key,
    coalesce(a.platform_key,b.platform_key,c.platform_key) platform_key,
    coalesce(a.product_key,b.product_key,c.product_key) product_key,
    coalesce(a.date,b.date,c.date) date,
    quantity,
    net_quantity,
    item_total_price,
    item_subtotal_price,
    item_shipping_price,
    item_giftwrap_price,
    item_discount,
    item_shipping_discount,
    item_total_tax,
    (ifnull(item_subtotal_price,0) - ifnull(adsales,0)) organic_sales,
    adsales,
    adspend,
    sessions,
    page_views
    from orders a
    full outer join advertising b
    on a.brand_key = b.brand_key and a.platform_key = b.platform_key and a.product_key = b.product_key and a.date = b.date 
    full outer join traffic c
    on a.brand_key = c.brand_key and a.platform_key = c.platform_key and a.product_key = c.product_key and a.date = c.date
    ) f
    left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') g
    on f.brand_key = g.brand_key
    left join {{ ref('dim_platform')}} h
    on f.platform_key = h.platform_key
    left join (select product_key, product_id, product_name, sku from {{ref('dim_product')}} where status = 'Active') i
    on f.product_key = i.product_key
    GROUP BY 1,2,3,4,5,6,7
)

select 
brand_name,
platform_name,
store_name,
product_id,
product_name,
sku,
month_start_date,
item_total_price as this_month_item_total_price,
LAG(item_total_price,1) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) last_month_item_total_price,
item_total_price - LAG(item_total_price,1) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) monthly_change_item_total_price,
LAG(item_total_price,12) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) last_year_item_total_price,
item_total_price - LAG(item_total_price,12) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) yearly_change_item_total_price,
organic_sales as this_month_organic_sales,
LAG(organic_sales,1) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) last_month_organic_sales,
organic_sales - LAG(organic_sales,1) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) monthly_change_organic_sales,
LAG(organic_sales,12) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) last_year_organic_sales,
organic_sales - LAG(organic_sales,12) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) yearly_change_organic_sales,
adsales as this_month_adsales,
LAG(adsales,1) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) last_month_adsales,
adsales - LAG(adsales,1) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) monthly_change_adsales,
LAG(adsales,12) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) last_year_adsales,
adsales - LAG(adsales,12) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) yearly_change_adsales,
adspend as this_month_adspend,
LAG(adspend,1) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) last_month_adspend,
adspend - LAG(adspend,1) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) monthly_change_adspend,
LAG(adspend,12) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) last_year_adspend,
adspend - LAG(adspend,12) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) yearly_change_adspend,
quantity as this_month_quantity,
LAG(quantity,1) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) last_month_quantity,
quantity - LAG(quantity,1) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) monthly_change_quantity,
LAG(quantity,12) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) last_year_quantity,
quantity - LAG(quantity,12) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) yearly_change_quantity,
net_quantity as this_month_net_quantity,
LAG(net_quantity,1) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) last_month_net_quantity,
net_quantity - LAG(net_quantity,1) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) monthly_change_net_quantity,
LAG(net_quantity,12) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) last_year_net_quantity,
net_quantity - LAG(net_quantity,12) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) yearly_change_net_quantity,
sessions as this_month_sessions,
LAG(sessions,1) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) last_month_sessions,
sessions - LAG(sessions,1) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) monthly_change_sessions,
LAG(sessions,12) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) last_year_sessions,
sessions - LAG(sessions,12) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) yearly_change_sessions,
page_views as this_month_page_views,
LAG(page_views,1) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) last_month_page_views,
page_views - LAG(page_views,1) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) monthly_change_page_views,
LAG(page_views,12) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) last_year_page_views,
page_views - LAG(page_views,12) OVER (PARTITION BY brand_name, platform_name, store_name, product_id, product_name, sku ORDER BY month_start_date) yearly_change_page_views
from aggregation
