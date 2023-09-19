{{config(materialized="table")}}

select
    brand,
    platform_name,
    store_name,
    customer_id,
    campaignName,
    campaignId,
    source,
    medium,
    sourceMedium,
    transactionId as order_id,
    purchaseRevenue,
    round(safe_divide(purchaseRevenue,sum(purchaseRevenue) over(partition by customer_id,transactionid)),2) as google_convertion_ratio,
    round(safe_divide(1,count(*) over(partition by customer_id,transactionid)),2) as linear_convertion_ratio
from {{ref('dim_sales_attribution')}}