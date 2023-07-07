
{% if var('product_details_gs_flag') %}
-- depends_on: {{ ref('ProductDetails') }}
{% endif %}

select prod.*,
{% if var('product_details_gs_flag') %}
description, 
category, 
sub_category, 
cast(mrp as numeric) mrp, 
cast(cogs as numeric) cogs, 
currency_code,
cast(start_date as date) date, 
cast(end_date as  date) date
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
coalesce(cast(id as string),'') product_id,
coalesce(variants_sku,'') sku,
coalesce(title,'') product_name, 
cast(null as string) as color, 
coalesce(vendor,'') seller,
cast(null as string) as size,
cast(null as string) product_category,
_daton_batch_runtime
from {{ ref('ShopifyProducts') }}) prod
{% if var('product_details_gs_flag') %}
left join (
  select sku, description,	category, sub_category, mrp, cogs, currency_code, start_date, end_date 
  from {{ ref('ProductDetails') }} 
  where lower(platform_name) = 'shopify') prod_gs
on prod.sku = prod_gs.sku
{% endif %}



