select * {{exclude()}} (row_num) from 
    (
    select
    'Amazon Seller Central' as platform_name,
    amazon_order_id as order_id,
    cast(null as string) as payment_mode,
    cast(null as string) as source,
    cast(null as string) as medium,
    cast(null as string) as campaign,
    'Online Marketplace' as order_channel,
    date(last_updated_date) as last_updated_date,
    row_number() over(partition by amazon_order_id order by last_updated_date desc) row_num
    from {{ref('FlatFileAllOrdersReportByLastUpdate')}} 
    )
where row_num = 1
