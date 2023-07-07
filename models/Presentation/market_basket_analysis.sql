SELECT 
    distinct
    date, 
    brand_name, 
    order_id,  
    e.platform_name, 
    e.store_name,
    product_name,
    regexp_replace(sku, '[^0-9]+', '') as sku
    from {{ ref('fact_order_lines')}} a
    left join {{ ref('dim_orders')}} c
    on a.order_key = c.order_key
    left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') d
    on a.brand_key = d.brand_key
    left join {{ ref('dim_platform')}} e
    on a.platform_key = e.platform_key
    left join (select product_key, product_id, product_name, sku from {{ref('dim_product')}} where status = 'Active') f
    on a.product_key = f.product_key