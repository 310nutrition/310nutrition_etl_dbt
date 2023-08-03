select * {{exclude()}} (row_num)
from (
    select *,
    row_number() over(partition by order_id,customer_id order by last_updated_date desc) row_num
    from 
        (
        select
        customer_id,
        email, 
        'Shopify' as acquisition_channel,
        date(created_at) as order_date,
        cast(order_id as string) as order_id,
        date(created_at) last_updated_date
        from {{ ref('ShopifyOrdersCustomer') }}

        union all

        select
        customers_id as customer_id,
        email, 
        'Shopify' as acquisition_channel,
        cast(null as date) as order_date,
        cast(null as string) as order_id,
        cast(null as date) last_updated_date
        from {{ ref('ShopifyCustomers') }}
        where customers_id not in (select distinct customer_id from {{ ref('ShopifyOrdersCustomer') }})
        ) 
    ) 
where row_num = 1

