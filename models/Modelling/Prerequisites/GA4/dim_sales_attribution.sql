{{config(materialized="table")}}

with GA4_attribution as (select 
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
    --start_date,
    --end_date,
    campaignName,
    campaignId,
    transactionId,
    source,
    medium,
    sourceMedium,
    purchaseRevenue
from {{ref('ga4_sales_attribution')}}),

order_map as (select distinct brand as brand,
    store ,
    order_id,
    name as order_name,
    customer_id
from {{ref('ShopifyOrdersCustomer')}}
where date(created_at) >= "2023-02-01")

select
    a.brand,
    a.platform_name,
    a.store_name,
    --dimensionHeaders,
    --metricHeaders,
    --start_date,
    --end_date,
    b.customer_id,
    campaignName,
    campaignId,
    b.order_id as transactionId,
    --b.order_id,
    source,
    medium,
    sourceMedium,
    purchaseRevenue
from GA4_attribution as a
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
    b.customer_id,
    campaignName,
    campaignId,
    a.transactionId as transactionId,
    --b.order_id,
    source,
    medium,
    sourceMedium,
    purchaseRevenue
from GA4_attribution as a
join order_map b
on a.brand = b.brand and a.store = b.store and a.transactionId = b.order_id
where length(a.transactionId) = 13