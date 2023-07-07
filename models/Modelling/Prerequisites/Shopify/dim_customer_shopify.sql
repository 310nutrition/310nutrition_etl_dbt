select 
date(created_at) as order_date,
order_id,
_daton_batch_runtime,
coalesce(email,'') as email,
'Shopify' as acquisition_channel
from {{ ref('ShopifyOrdersCustomer') }}

union all

select
cast(null as date) as order_date,
cast(null as string) as order_id,
_daton_batch_runtime,
coalesce(email,'') as email,
'null' as acquisition_channel
from {{ ref('ShopifyCustomers') }}
where email not in (select distinct email from {{ ref('ShopifyOrdersCustomer') }})