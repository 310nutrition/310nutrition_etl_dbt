
{% if var('product_details_gs_flag') %}
-- depends_on: {{ ref('ProductDetails') }}
{% endif %}

select products.*,
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
  select 
  distinct 
  'Amazon Seller Central' as platform_name,
  coalesce(asin1,'') product_id,
  coalesce(seller_sku,'') sku,
  coalesce(item_name,'') product_name, 
  coalesce(colorName,'') color,
  coalesce(manufacturer,'') seller,
  coalesce(sizeName,'') size,
  coalesce(displayGroupRanks_title,'') product_category,
  _daton_batch_runtime
  from {{ ref('AllListingsReport') }} alllistings
  left join (select ReferenceASIN, colorName, manufacturer, sizeName, displayGroupRanks_title from {{ ref('CatalogItems') }}) catalogitems
  on alllistings.asin1 = catalogitems.ReferenceASIN
) products

{% if var('product_details_gs_flag') %}
left join (
  select 
  sku, 
  description,	
  category, 
  sub_category, 
  mrp, 
  cogs,
  currency_code, 
  start_date, 
  end_date 
  from {{ ref('ProductDetails') }} 
  where lower(platform_name) = 'amazon') prod_gs
on products.sku = prod_gs.sku
{% endif %}

