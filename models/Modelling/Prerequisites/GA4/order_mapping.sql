{{config(materialized="table")}}

with sales_source as (select 
    brand,
    platform_name,
    case
        when store_name = 'US' then 'United States'
        when store_name = 'NA' then 'Canada'
        end as store_name,
    case
        when store_name = 'US' then 'US'
        when store_name = 'NA' then 'CA'
        end as store,
    --dimensionHeaders,
    --metricHeaders,
    date,
    --start_date,
    --end_date,
    transactionId,
    source,
    medium,
    sourceMedium,
    Mapped_source,
    purchaseRevenue
from {{ref('ga4_sales_sourcemedium')}}),

order_map as (select distinct brand as brand,
    store ,
    order_id,
    name as order_name,
    customer_id
from {{ref('ShopifyOrdersCustomer')}}
where date(created_at) >= "2023-01-01"),

cte1 as (select
    a.brand,
    a.platform_name,
    a.store_name,
    --dimensionHeaders,
    --metricHeaders,
    date,
    --start_date,
    --end_date,
    b.customer_id,
    b.order_id as transactionId,
    --b.order_id,
    source,
    medium,
    sourceMedium,
    Mapped_source,
    purchaseRevenue
from sales_source as a
join order_map b
on a.brand = b.brand and a.store = b.store and a.transactionId = b.order_name
where length(a.transactionId) = 8

union all

select
    a.brand,
    a.platform_name,
    a.store_name,
    --dimensionHeaders,
    --metricHeaders,
    --start_date,
    --end_date,
    date,
    b.customer_id,
    --campaignName,
    --campaignId,
    a.transactionId as transactionId,
    --b.order_id,
    source,
    medium,
    sourceMedium,
    Mapped_source,
    purchaseRevenue
from sales_source as a
join order_map b
on a.brand = b.brand and a.store = b.store and a.transactionId = b.order_id
where length(a.transactionId) = 13),

cte2 as (select
    brand,
    platform_name,
    store_name,
    date,
    customer_id,
    transactionId,
    Mapped_source,
    sum(purchaseRevenue) as purchaseRevenue
from cte1
group by 1,2,3,4,5,6,7)

select
    brand,
    platform_name,
    store_name,
    date,
    transactionId as order_id,
    customer_id,
    Mapped_source,
    purchaseRevenue,
    round(safe_divide(purchaseRevenue,sum(purchaseRevenue) over(partition by customer_id,transactionid)),2) as google_convertion_ratio,
    round(safe_divide(1,count(*) over(partition by customer_id,transactionid)),2) as linear_convertion_ratio
from cte2