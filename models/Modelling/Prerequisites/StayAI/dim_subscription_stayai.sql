select 
brand,
'Shopify' as platform_name,
'Stay_AI' as order_channel,
{{store_name('store')}},
a.subscription_id,
cast(customerId as numeric) as customer_id,
cast(null as string) as utm_source,
cast(null as string) as utm_medium,
date(createdat)  as created_at,
line_item_shopify_product_id as external_product_id,
line_item_shopify_variant_id as external_variant_id,
date(nextBillingDate) as next_charge_scheduled_at,
cast(orderIntervalFrequency as string) as order_interval_frequency,
cast(orderIntervalUnit as string) as order_interval_unit,
line_item_product_title as product_title,
line_item_quantity as quantity,
line_item_sku as sku,
status,
date(updatedAt)  as updated_at,
cancellationReason as cancellation_reason,
cancelledAt as cancelled_at,
--order_day_of_month,
currency as presentment_currency,
cancellationReason as cancellation_reason_comments,
_daton_batch_runtime 
from {{ ref('StayAIOrdersLineItems') }} a left join 
(select distinct customerid, subscription_id,nextBillingDate,orderIntervalFrequency,orderIntervalUnit,cancellationReason,cancelledAt,status   from {{ref('StayAISubscriptions')}}) b on a.subscription_id = b.subscription_id
--left join (select distinct line_item_shopify_variant_id,line_item_sku from {{ ref('StayAIOrdersLineItems') }}) prod on subs.line_item_variant_id = cast(prod.line_item_shopify_variant_id as string)