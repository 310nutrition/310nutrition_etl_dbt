
{% if var('product_details_gs_flag') %}
-- depends_on: {{ ref('ProductDetails') }}
{% endif %}

SELECT * {{exclude()}} (row_num) from
(
select prod.*,
{% if var('product_details_gs_flag') %}
description, 
category, 
sub_category, 
cast(mrp as numeric) mrp, 
cast(cogs as numeric) cogs, 
currency_code,
cast(start_date as date) start_date, 
cast(end_date as  date) end_date
{% else %}
cast(null as string) as description, 
cast(null as string) as category, 
cast(null as string) as sub_category, 
cast(null as numeric) as mrp, 
cast(null as numeric) as cogs, 
cast(null as string) as currency_code,
cast(null as date) as start_date, 
cast(null as date) as end_date 
{% endif %} 
from (
select distinct
'Shopify' as platform_name,
coalesce(cast(line_items_product_id as string),'') product_id,
coalesce(line_items_sku,'') sku,
coalesce(line_items_name,'') product_name, 
cast(null as string) as color, 
'' as seller,
cast(null as string) as size,
cast(null as string) product_category,
_daton_batch_runtime,
row_number() over(partition by line_items_product_id, line_items_sku order by _daton_batch_runtime desc) as row_num
from {{ ref('ShopifyOrdersLineItems') }}) prod
{% if var('product_details_gs_flag') %}
left join (
  select sku, description,	category, sub_category, mrp, cogs, currency_code, start_date, end_date 
  from {{ ref('ProductDetails') }} 
  where lower(platform_name) = 'shopify') prod_gs
on prod.sku = prod_gs.sku
{% endif %}
)
where row_num = 1



