select * {{exclude()}} (row_num)
from (
    select *,
    row_number() over(partition by order_id,customer_id order by last_updated_date desc) row_num
    from 
        (
        select
        coalesce(ord_cust.customer_id,cust.customers_id) as customer_id,
        coalesce(cust.email,ord_cust.email) as email, 
        'Shopify' as acquisition_channel,
        date(coalesce(ord_cust.created_at,cust.created_at)) as order_date,
        cast(order_id as string) as order_id,
        date(coalesce(ord_cust.created_at,cust.created_at)) last_updated_date
        from {{ ref('ShopifyOrdersCustomer') }} ord_cust
        full outer join {{ ref('ShopifyCustomers') }} cust on ord_cust.customer_id= cust.customers_id 
        --union all

        --select
        --customers_id as customer_id,
        --email, 
        --'Shopify' as acquisition_channel,
        --cast(null as date) as order_date,
        --cast(null as string) as order_id,
        --cast(null as date) last_updated_date
        --from {#{ ref('ShopifyCustomers') }#}
        --where customers_id not in (select distinct customer_id from {#{ ref('ShopifyOrdersCustomer') }#})
        
        ) 
    ) 
where row_num = 1

