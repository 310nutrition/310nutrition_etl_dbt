select 
'Shopify' as platform_name,
cast(null as string) type,
{{store_name('store')}},
cast(null as string) description,
cast(null as string) status,
_daton_batch_runtime
from {{ ref('ShopifyOrders') }}