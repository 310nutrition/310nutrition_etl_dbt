select * {{exclude()}} (row_num) from 
    (
    select
    'Shopify' as platform_name,
    cast(null as string) type,
    {{ store_name('store') }},
    cast(null as string) description,
    cast(null as string) status,
    date(updated_at) as last_updated_date,
    row_number() over(partition by store order by date(updated_at) desc) row_num
    from {{ref('ShopifyOrders')}} 
    )
where row_num = 1

