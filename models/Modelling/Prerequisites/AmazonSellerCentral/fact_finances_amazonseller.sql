select 
brand,
date,
amount_type,
transaction_type,
coalesce(charge_type,'') charge_type,
amazonorderid as order_id,
store_name,
asin1 as product_id,
sellerSKU as sku,
exchange_currency_code,
platform_name,
amount
from (
    select 
    brand,
    date(posteddate) as date,
    'Fees' as amount_type,
    'Order' as transaction_type,
    FeeType as charge_type,
    amazonorderid,
    {{ store_name('marketplacename') }},
    sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((CurrencyAmount/exchange_currency_rate),2)) amount
    from {{ ref('ListFinancialEventsOrderFees')}}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL

    select 
    brand,
    date(posteddate) as date,
    'Promotional Discount' as amount_type,
    'Order' as transaction_type,
    PromotionType as charge_type,
    amazonorderid,
    {{ store_name('marketplacename') }},
    sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((CurrencyAmount/exchange_currency_rate),2)) amount
    from {{ ref('ListFinancialEventsOrderPromotions')}}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL

    select
    brand,
    date(posteddate) as date,
    'Revenue' as amount_type,
    'Order' as transaction_type,
    ChargeType as charge_type,
    amazonorderid,
    {{ store_name('marketplacename') }},
    sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((CurrencyAmount/exchange_currency_rate),2)) amount
    from {{ ref('ListFinancialEventsOrderRevenue')}}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL

    select 
    brand,
    date(posteddate) as date,
    'Taxes' as amount_type,
    'Order' as transaction_type,
    ChargeType as charge_type,
    amazonorderid,
    {{ store_name('marketplacename') }},
    sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((CurrencyAmount/exchange_currency_rate),2)) amount
    from {{ ref('ListFinancialEventsOrderTaxes')}}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL

    select
    brand,
    date(posteddate) as date,
    'Fees' as amount_type,
    'Refund' as transaction_type,
    FeeType as charge_type,
    amazonorderid,
    {{ store_name('marketplacename') }},
    sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((CurrencyAmount/exchange_currency_rate),2)) amount
    from {{ ref('ListFinancialEventsRefundFees')}}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL

    select
    brand,
    date(posteddate) as date,
    'Promotional Discount' as amount_type,
    'Refund' as transactionType,
    PromotionType as charge_type,
    amazonorderid,
    {{ store_name('marketplacename') }},
    sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((CurrencyAmount/exchange_currency_rate),2)) amount
    from {{ ref('ListFinancialEventsRefundPromotions')}}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL

    select 
    brand,
    date(posteddate) as date,
    'Revenue' as amount_type,
    'Refund' as transaction_type,
    ChargeType as charge_type,
    amazonorderid,
    {{ store_name('marketplacename') }},
    sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((CurrencyAmount/exchange_currency_rate),2)) amount
    from {{ ref('ListFinancialEventsRefundRevenue')}}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL

    select
    brand,
    date(posteddate) as date,
    'Taxes' as amount_type,
    'Refund' as transaction_type,
    ChargeType as charge_type,
    amazonorderid,
    {{ store_name('marketplacename') }},
    sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((CurrencyAmount/exchange_currency_rate),2)) amount
    from {{ ref('ListFinancialEventsRefundTaxes')}}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL 

    select
    brand,
    date(RequestStartDate) as date,
    'Service Fees' as amount_type,
    'Order' as transaction_type,
    'Service Level Fees' as charge_type,
    amazonorderid,
    {{ store_name('marketplaceName') }},
    sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((CurrencyAmount/exchange_currency_rate),2)) amount
    from {{ ref('ListFinancialEventsServiceFees')}}
    where exchange_currency_code is not null
    group by 1,2,3,4,5,6,7,8,9,10

) lfe
left join (select distinct seller_sku, asin1 from {{ ref('AllListingsReport')}}) listings
on lfe.sellerSKU = listings.seller_sku