select
'Shopify' as platform_name,
order_id,
id as transaction_id,
kind as transaction_stage,
gateway as payment_gateway,
message,
cast(null as string) as payment_mode,
status as payment_status,
_daton_batch_runtime
from {{ ref('ShopifyTransactions') }} 