select * {{exclude()}} (row_num) from 
    (
    select
    'Amazon Seller Central' as platform_name,
    amazon_order_id as order_id,
    cast(null as string) as payment_mode,
    'Online Marketplace' as order_channel,
    date(last_updated_date) as last_updated_date,
    row_number() over(partition by amazon_order_id order by _daton_batch_runtime desc) row_num
    from {{ref('FlatFileAllOrdersReportByLastUpdate')}} 
    ) orders
where row_num = 1
