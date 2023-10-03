{{config(materialized="table")}}

with cum_sales as (select
    brand_name,
    platform_name,
    store_name,
    date,
    order_id,
    email,
    customer_id,
    customer_first_order_date as acquisition_date,
    order_type,
    sum(coalesce(item_subtotal_price,0)) as subtotal_price,
    sum(coalesce(item_taxes,0)) as item_taxes,
    sum(coalesce(item_shipping,0)) as item_shipping,
    sum(coalesce(item_shipping_tax,0)) as item_shipping_tax,
    sum(coalesce(item_shipping_discount,0)) as item_shipping_discount,
    sum(coalesce(item_giftwrap,0)) as item_giftwrap,
    sum(coalesce(item_discount,0)) as item_discount,
    sum(coalesce(item_refund_amount,0)) as item_refund_amount
from {{ref('custom_sales')}}
where platform_name = 'Shopify'
group by 1,2,3,4,5,6,7,8,9),


first_attribution as (select * 
from (select
    a.brand,
    a.platform_name,
    a.store_name,
    a.customer_id,
    a.campaignName,
    a.campaignId,
    a.source,
    a.medium,
    a.sourceMedium,
    a.order_id,
    b.date as order_date,
    b.acquisition_date,
    a.purchaseRevenue,
    a.google_convertion_ratio,
    a.linear_convertion_ratio
from {{ref('fact_sales_attribution')}} a 
left join cum_sales as b
on a.brand = b.brand_name and a.platform_name = b.platform_name and a.customer_id = b.customer_id and a.order_id = b.order_id)
where order_date = acquisition_date)

select 
    a.brand_name,
    a.platform_name,
    a.store_name,
    a.date,
    a.email,
    a.customer_id,
    a.acquisition_date,
    coalesce(b.campaignName,'Unattributed') as first_attributed_campaignName,
    coalesce(b.campaignID,'Unattributed') as first_attributed_campaignId,
    coalesce(b.source,'Unattributed') as first_attributed_source,
    coalesce(b.medium,'Unattributed') as first_attributed_medium,
    coalesce(b.sourceMedium,'Unattributed') as first_attributed_sourceMedium,
    a.order_id,
    a.order_type,
    a.subtotal_price,
    a.subtotal_price + item_taxes + item_shipping + item_shipping_tax - item_shipping_discount + item_giftwrap - item_discount - item_refund_amount as net_sales,
    coalesce(b.google_convertion_ratio,1) as first_attributed_google_convertion_ratio,
    coalesce(b.linear_convertion_ratio,1) as first_attributed_linear_convertion_ratio
from cum_sales a 
left join first_attribution b
on a.brand_name = b.brand and a.platform_name = b.platform_name and a.store_name=b.store_name and a.customer_id = b.customer_id