select
'Shopify' as platform_name,
a.order_id,
cast(a.line_items_sku as string) as sku,
cast(a.line_items_product_id as string) as product_id,
b.shipping_lines_id as shipment_id,
b.shipping_lines_id as shipment_item_id,
a.fulfillments_service as fulfillment_channel,
cast(null as string) as fulfillment_center_id,
cast(null as date) as shipment_date,
b.shipping_lines_carrier_identifier as carrier,
cast(null as date) estimated_delivery_date,
cast(null as date) delivered_at,
a.fulfillments_tracking_number as tracking_number,
a._daton_batch_runtime
from {{ ref('ShopifyOrdersFulfillments') }} a
left join (select distinct order_id, shipping_lines_id, shipping_lines_carrier_identifier from {{ ref('ShopifyOrdersShippingLines')}}) b 
on a.order_id = b.order_id
