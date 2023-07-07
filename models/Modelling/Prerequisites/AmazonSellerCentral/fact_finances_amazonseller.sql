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

    UNION ALL 

    select
    brand,
    date(reportDate) as date,
    'Advertising Spend' as amount_type,
    'Order' as transaction_type,
    'Sponsored Product Advertising Spend' as charge_type,
    cast(null as string) as amazonorderid,
    {{ store_name('countryName') }},
    sku as sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((0-cost/exchange_currency_rate),2)) amount
    from {{ ref('SPProductAdsReport')}}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL 

    select
    brand,
    date(reportDate) as date,
    'Advertising Spend' as amount_type,
    'Order' as transaction_type,
    'Sponsored Display Advertising Spend' as charge_type,
    cast(null as string) as amazonorderid,
    {{ store_name('countryName') }},
    sku as sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((0-cost/exchange_currency_rate),2)) amount
    from {{ ref('SDProductAdsReport')}}
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL 

    select
    brand,
    date(reportDate) as date,
    'Advertising Spend' as amount_type,
    'Order' as transaction_type,
    'Sponsored Brand Advertising Spend' as charge_type,
    cast(null as string) as amazonorderid,
    {{ store_name('countryName') }},
    cast(null as string) as sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((0-cost/exchange_currency_rate),2)) amount
    from (
        select *,
        {% if var('currency_conversion_flag') %}
        case when b.value is null then 1 else b.value end as exchange_currency_rate,
        case when b.from_currency_code is null then a.currency else b.from_currency_code end as exchange_currency_code,
        {% else %}
        cast(1 as decimal) as exchange_currency_rate,
        cast(a.currency as string) as exchange_currency_code, 
        {% endif %}
        from (
            select *,
            {{currency_code('countryName')}} 
            from {{ ref('SBAdGroupsReport')}} 
            ) a 
        {% if var('currency_conversion_flag') %}
        left join {{ ref('ExchangeRates')}} b on date(a.reportDate) = b.date and a.currency = b.to_currency_code
        {% endif %}
        ) 
    
    group by 1,2,3,4,5,6,7,8,9,10

    UNION ALL 

    select
    brand,
    date(reportDate) as date,
    'Advertising Spend' as amount_type,
    'Order' as transaction_type,
    'Sponsored Brand Videos Advertising Spend' as charge_type,
    cast(null as string) as amazonorderid,
    {{ store_name('countryName') }},
    cast(null as string) as sellerSKU,
    exchange_currency_code,
    'Amazon Seller Central' as platform_name,
    sum(round((0-cost/exchange_currency_rate),2)) amount
    from (
        select *,
        {% if var('currency_conversion_flag') %}
        case when b.value is null then 1 else b.value end as exchange_currency_rate,
        case when b.from_currency_code is null then a.currency else b.from_currency_code end as exchange_currency_code,
        {% else %}
        cast(1 as decimal) as exchange_currency_rate,
        cast(a.currency as string) as exchange_currency_code, 
        {% endif %}
        from (
            select *,
            {{currency_code('countryName')}} 
            from {{ ref('SBAdGroupsVideoReport')}} 
            ) a
        {% if var('currency_conversion_flag') %}
        left join {{ ref('ExchangeRates')}} b on date(a.reportDate) = b.date and a.currency = b.to_currency_code
        {% endif %}
        )
    group by 1,2,3,4,5,6,7,8,9,10

) lfe
left join (select distinct seller_sku, asin1 from {{ ref('AllListingsReport')}}) listings
on lfe.sellerSKU = listings.seller_sku