-- Shopify Forward Fees
    select 
    brand,
    date(processed_at) as date,
    'Fees' as amount_type,
    'Order' as transaction_type,
    coalesce(kind,'') charge_type,
    cast(order_id as string) order_id,
    {{store_name('store')}},
    '' as product_id,
    '' as sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round((cast(amount as numeric)/exchange_currency_rate),2)) amount
    from {{ ref('ShopifyTransactions')}}
    group by 1,2,3,4,5,6,7,8,9,10,11

    UNION ALL

-- Shopify Forward Item Price
    select 
    brand,
    date(created_at) as date,
    'Revenue' as amount_type,
    'Order' as transaction_type,
    'Item Price' as charge_type,
    order_id,
    {{store_name('store')}},
    cast(line_items_product_id as string) product_id,
    line_items_sku as sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round((CAST(line_items_price*line_items_quantity as numeric)/exchange_currency_rate),2)) amount
    from {{ ref('ShopifyOrdersLineItems')}}
    group by 1,2,3,4,5,6,7,8,9,10,11

    UNION ALL

    -- Shopify Forward Taxes
    select 
    brand,
    date(created_at) as date,
    'Taxes' as amount_type,
    'Order' as transaction_type,
    'Taxes' as charge_type,
    order_id,
    {{store_name('store')}},
    '' as product_id,
    '' as sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round((cast(total_tax as numeric)/exchange_currency_rate),2)) amount
    from {{ ref('ShopifyOrders')}}
    group by 1,2,3,4,5,6,7,8,9,10,11

    UNION ALL
    
    -- Shopify Forward Shipping    
    select 
    brand,
    date(created_at) as date,
    'Shipping' as amount_type,
    'Order' as transaction_type,
    coalesce(shipping_lines_title,'') as charge_type,
    order_id,
    {{store_name('store')}},
    '' as product_id,
    '' as sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round((cast(shipping_lines_discounted_price as numeric)/exchange_currency_rate),2)) amount
    from {{ ref('ShopifyOrdersShippingLines')}}
    group by 1,2,3,4,5,6,7,8,9,10,11

    UNION ALL

    -- Shopify Forward Shipping Promotions
    select 
    brand,
    date(created_at) as date,
    'Shipping Promotional Discount' as amount_type,
    'Order' as transaction_type,
    coalesce(discount_type,'') as charge_type,
    order_id,
    {{store_name('store')}},
    '' product_id,
    '' as sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round((cast(discount_amount as numeric)/exchange_currency_rate),2)) amount
    from {{ ref('ShopifyOrders')}}
    where discount_type = 'shipping'
    group by 1,2,3,4,5,6,7,8,9,10,11

    UNION ALL

-- Shopify Forward Item Promotions
    select 
    brand,
    date(created_at) as date,
    'Item Promotional Discount' as amount_type,
    'Order' as transaction_type,
    coalesce(discount_type,'') as charge_type,
    order_id,
    {{store_name('store')}},
    '' product_id,
    '' as sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round((cast(discount_amount as numeric)/exchange_currency_rate),2)) amount
    from {{ ref('ShopifyOrders')}}
    where discount_type != 'shipping'
    group by 1,2,3,4,5,6,7,8,9,10,11


    UNION ALL

-- Shopify Reverse Fees
    select 
    brand,
    date(processed_at) as date,
    'Fees' as amount_type,
    'Refund' as transaction_type,
    coalesce(kind,'') charge_type,
    cast(order_id as string) order_id,
    {{store_name('store')}},
    '' as product_id,
    '' as sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round((cast(amount as numeric)/exchange_currency_rate),2)) amount 
    from {{ ref('ShopifyTransactions')}}
    where kind = 'refund'
    group by 1,2,3,4,5,6,7,8,9,10,11

    UNION ALL

-- Shopify Reverse Promotions 
    select 
    brand,
    date(created_at) as date,
    'Promotional Discount' as amount_type,
    'Refund' as transaction_type,
    'Promotional Discount' as charge_type,
    cast(refund_id as string) order_id,
    {{store_name('store')}},
    cast(line_item_product_id as string) product_id,
    line_item_sku as sku,
    ref_ln_itms.exchange_currency_code, 
    'Shopify' as platform_name,
    sum(round((cast(line_item_total_discount as numeric)/exchange_currency_rate),2)) amount
    from {{ ref('ShopifyRefundsLineItems')}} ref_ln_itms
    group by 1,2,3,4,5,6,7,8,9,10,11


    UNION ALL

-- Shopify Reverse Item Price and Shipping
    select 
    brand,
    date(created_at) as date,
    'Revenue' as amount_type,
    'Refund' as transaction_type,
    'Refund Revenue' as charge_type,
    cast(refund_id as string) as order_id,
    {{store_name('store')}},
    '' as product_id,
    '' as sku,
    exchange_currency_code,
    'Shopify' as platform_name,
    sum(round((cast(transactions_amount as numeric)/exchange_currency_rate),2)) amount
    from {{ ref('ShopifyRefundsTransactions')}}
    where created_at is not null
    group by 1,2,3,4,5,6,7,8,9,10,11

    UNION ALL

--  Shopify Reverse Taxes
    select 
    brand,
    date(created_at) as date,
    'Taxes' as amount_type,
    'Refund' as transaction_type,
    'Shopify Taxes' as charge_type,
    cast(refund_id as string) order_id,
    {{store_name('store')}},
    cast(line_item_product_id as string) product_id,
    line_item_sku as sku,
    exchange_currency_code, 
    'Shopify' as platform_name,
    sum(round((cast(tax_lines_price as numeric)/exchange_currency_rate),2)) amount
    from {{ ref('ShopifyRefundLineItemsTax')}} ref_ln_itms_tax

    group by 1,2,3,4,5,6,7,8,9,10,11
    
