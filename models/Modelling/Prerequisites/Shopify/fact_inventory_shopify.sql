select 
brand,
order_platform,
store_name,
cast (null as string) as fulfillment_center_id,
'Merchant' as fulfillment_channel,
product_id, 
sku, 
date(updated_at) as date,
'Customer Shipment' as event,
'' as type,
sum(value) value
from (
select *,
cast(coalesce(cast(a.inventory_levels_available as int),0) as int) value,
cast(b.variants_product_id as string) as product_id, 
b.variants_sku as sku,
'Shopify' as order_platform,
{{store_name('store')}}
from {{ ref('ShopifyInventoryLevels')}} a
left join (select variants_product_id, variants_inventory_item_id,variants_sku from {{ ref('ShopifyProducts') }}) b
on a.inventory_item_id = b.variants_inventory_item_id
where inventory_levels_available is not null) 
where value != 0
group by 1,2,3,4,5,6,7,8
