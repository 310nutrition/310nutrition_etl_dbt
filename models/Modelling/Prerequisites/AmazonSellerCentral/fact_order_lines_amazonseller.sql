select 
amazon_order_id as order_id,
brand,
'Amazon Seller Central' as platform_name,
{{ store_name('sales_channel') }},
asin as product_id, 
sku,
currency,
exchange_currency_code,
exchange_currency_rate,
date(purchase_date) as date,
cast(null as string) as subscription_id,
'Order' as transaction_type,
case when lower(order_status) = 'cancelled' or lower(item_status) = 'cancelled' then true else false end as is_cancelled,
cast(null as string) as reason,
b.buyeremail as email,
sum(quantity) quantity,
sum(ifnull(item_price,0) + ifnull(item_tax,0) + ifnull(shipping_tax,0) + ifnull(gift_wrap_tax,0)) total_price,
sum(item_price) subtotal_price,
sum(ifnull(item_tax,0) + ifnull(shipping_tax,0) + ifnull(gift_wrap_tax,0)) total_tax, 
sum(shipping_price) shipping_price, 
sum(gift_wrap_price) giftwrap_price,
sum(item_promotion_discount) item_discount,
sum(ship_promotion_discount) shipping_discount 
from (
    select *
    from {{ ref('FlatFileAllOrdersReportByLastUpdate') }}
    where sales_channel <> 'Non-Amazon' 
    ) main 
left join (select distinct amazonorderid, buyeremail from {{ ref('ListOrder') }}) b
on main.amazon_order_id = b.amazonorderid
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

union all

select 
order_id,
brand,
'Amazon Seller Central' as platform_name,
store_name,
product_id, 
sku,
currency,
exchange_currency_code,
exchange_currency_rate,
date,
subscription_id,
'Return' as transaction_type,  
is_cancelled,
return_reason as reason,
email,
quantity,
total_price,
subtotal_price, 
total_tax, 
shipping_price, 
giftwrap_price,
item_discount,
shipping_discount  
from (
    select 
    ret.order_id,
    brand,
    {{ store_name('marketplaceName') }},
    ret.asin as product_id, 
    ret.sku,
    ord.currency,
    ord.exchange_currency_code,
    ord.exchange_currency_rate,
    date(return_date) as date,
    reason as return_reason,
    cast(null as string) as subscription_id, 
    false as is_cancelled,
    sum(ret.quantity) as quantity,
    sum(((ifnull(item_price,0) + ifnull(item_tax,0))/nullif(ord.quantity,0)) * ret.quantity) as total_price,
    cast(null as numeric) as subtotal_price, 
    cast(null as numeric) as total_tax, 
    cast(null as numeric) as shipping_price, 
    cast(null as numeric) as giftwrap_price,
    cast(null as numeric) as item_discount,
    cast(null as numeric) as shipping_discount 
    from {{ ref('FBAReturnsReport') }} ret
    left join (
        select amazon_order_id, sku, currency, exchange_currency_rate, exchange_currency_code, 
        sum(item_price) as item_price, sum(item_tax) item_tax, sum(quantity) as quantity
        from {{ ref('FlatFileAllOrdersReportByLastUpdate') }}
        where item_status != 'Cancelled'
        group by 1,2,3,4,5) ord
    on ret.order_id = ord.amazon_order_id and ret.sku = ord.sku
    group by 1,2,3,4,5,6,7,8,9,10,11,12
    
    UNION ALL

    select
    order_id,
    brand,
    {{ store_name('marketplaceName') }},
    asin as product_id, 
    Merchant_SKU as sku,
    Currency_code as currency,
    exchange_currency_code,
    exchange_currency_rate,
    date(Return_request_date) as date,
    Return_Reason as return_reason,
    cast(null as string) as subscription_id,
    false as is_cancelled,
    sum(Return_quantity) as quantity,
    sum(Refunded_Amount) as total_price,
    cast(null as numeric) as subtotal_price, 
    cast(null as numeric) as total_tax, 
    cast(null as numeric) as shipping_price, 
    cast(null as numeric) as giftwrap_price,
    cast(null as numeric) as item_discount,
    cast(null as numeric) as shipping_discount 
    from {{ref('FlatFileReturnsReportByReturnDate')}}a
    left join (
        select amazon_order_id, sku, currency, exchange_currency_rate, exchange_currency_code, 
        sum(item_price) as item_price, sum(item_tax) item_tax, sum(quantity) as quantity
        from {{ ref('FlatFileAllOrdersReportByLastUpdate') }}
        where item_status != 'Cancelled'
        group by 1,2,3,4,5) ord
    on a.order_id = ord.amazon_order_id and a.Merchant_SKU = ord.sku
    group by 1,2,3,4,5,6,7,8,9,10,11,12
    ) rr 
left join
(select distinct amazonorderid, buyeremail as email from {{ ref('ListOrder') }}) lo
on rr.order_id = lo.amazonorderid