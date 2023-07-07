select
coalesce(buyeremail,'') email, 
'Amazon Seller Central' as acquisition_channel,
date(PurchaseDate) as order_date,
amazonorderid as order_id,
_daton_batch_runtime
from {{ ref('ListOrder') }}