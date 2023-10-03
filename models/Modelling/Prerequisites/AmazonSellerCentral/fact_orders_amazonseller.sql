select all_ord.*, lst_ord.buyeremail as customer_id
  from (
    select
    amazon_order_id as order_id,
    brand,
    'Amazon Seller Central' as platform_name,
    {{ store_name('sales_channel') }},
    currency,
    exchange_currency_code,
    exchange_currency_rate,
    date(purchase_date) as date,
    case when lower(order_status) = 'cancelled' then 'Cancelled' else 'Order' end as transaction_type,
    sum(quantity) quantity,
    sum(ifnull(item_price,0) + ifnull(item_tax,0) + ifnull(shipping_tax,0) + ifnull(gift_wrap_tax,0)) total_price,
    sum(item_price) subtotal_price,
    sum(ifnull(item_tax,0) + ifnull(shipping_tax,0) + ifnull(gift_wrap_tax,0)) total_tax, 
    sum(shipping_price) shipping_price, 
    sum(gift_wrap_price) giftwrap_price,
    sum(item_promotion_discount) order_discount,
    sum(ship_promotion_discount) shipping_discount
    from {{ ref('FlatFileAllOrdersReportByLastUpdate') }}
    where sales_channel <> 'Non-Amazon' and item_status != 'Cancelled'
    group by 1,2,3,4,5,6,7,8,9
    ) all_ord 
  left join (select distinct amazonorderid, buyeremail from {{ ref('ListOrder') }}) lst_ord
  on all_ord.order_id = lst_ord.amazonorderid
    
union all

select rtrns.*, list_ord.email as customer_id
from (
    select 
    fba_rtrn.order_id,
    brand,
    'Amazon Seller Central' as platform_name,
    {{ store_name('marketplaceName') }},
    ord.currency,
    ord.exchange_currency_code,
    ord.exchange_currency_rate,
    date(return_date) as date,
    'Return' as transaction_type,  
    coalesce(sum(fba_rtrn.quantity),cast(null as numeric)) quantity,
    coalesce(sum((ifnull(item_price,0) + ifnull(item_tax,0))/nullif(ord.quantity,0)*fba_rtrn.quantity),cast(null as numeric)) total_price,
    cast(null as numeric) as subtotal_price,
    cast(null as numeric) as total_tax,
    cast(null as numeric) as shipping_price,
    cast(null as numeric) as giftwrap_price,
    cast(null as numeric) as order_discount,
    cast(null as numeric) as shipping_discount
    from {{ ref('FBAReturnsReport') }} fba_rtrn
    left join (
        select amazon_order_id, currency, exchange_currency_rate, exchange_currency_code, 
        sum(item_price) as item_price, sum(item_tax) item_tax, sum(quantity) as quantity
        from {{ ref('FlatFileAllOrdersReportByLastUpdate') }}
        where item_status != 'Cancelled'
        group by 1,2,3,4) ord
    on fba_rtrn.order_id = ord.amazon_order_id
    group by 1,2,3,4,5,6,7,8,9
    
    UNION ALL

    select
    order_id,
    brand,
    'Amazon Seller Central' as platform_name,
    {{ store_name('marketplaceName') }},
    Currency_code as currency,
    exchange_currency_code,
    exchange_currency_rate,
    date(Return_request_date) as date,
    'Return' as transaction_type,
    coalesce(sum(Return_quantity),cast(null as numeric)) quantity,
    coalesce(sum(Refunded_Amount),cast(null as numeric)) total_price,
    cast(null as numeric) as subtotal_price,
    cast(null as numeric) as total_tax,
    cast(null as numeric) as shipping_price,
    cast(null as numeric) as giftwrap_price,
    cast(null as numeric) as order_discount,
    cast(null as numeric) as shipping_discount
    from {{ref('FlatFileReturnsReportByReturnDate')}}
    group by 1,2,3,4,5,6,7,8,9
    ) rtrns 
left join
(select distinct amazonorderid, buyeremail as email from {{ ref('ListOrder') }}) list_ord
on rtrns.order_id = list_ord.amazonorderid