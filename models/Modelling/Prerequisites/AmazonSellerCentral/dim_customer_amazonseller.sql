select
distinct
coalesce(buyeremail,'') customer_id, 
coalesce(buyeremail,'') email, 
'Amazon Seller Central' as acquisition_channel,
date(PurchaseDate) as order_date,
amazonorderid as order_id,
date(LastUpdateDate) last_updated_date
from {{ ref('ListOrder') }}