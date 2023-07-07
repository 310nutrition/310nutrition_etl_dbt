select
'Amazon Seller Central' as platform_name,
a.amazon_order_id as order_id,
a.sku,
b.asin1 as product_id,
a.shipment_id,
a.shipment_item_id,
'Amazon' as fulfillment_channel,
a.fulfillment_center_id,
cast(null as date) as shipment_date,
a.carrier,
cast(substring(a.estimated_arrival_date,1,10) as date) estimated_delivery_date,
cast(null as date) delivered_at,
a.tracking_number,
a._daton_batch_runtime
from {{ ref('FBAAmazonFulfilledShipmentsReport') }} a
left join (select distinct asin1, seller_sku from {{ ref('AllListingsReport')}}) b 
on a.sku = b.seller_sku