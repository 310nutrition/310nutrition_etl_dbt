select * {{exclude()}} (row_num) from 
    (
    select
    'Amazon Seller Central' as platform_name,
    cast(null as string) type,
    {{ store_name('sales_channel') }},
    cast(null as string) description,
    cast(null as string) status,
    date(last_updated_date) as last_updated_date,
    row_number() over(partition by sales_channel order by date(last_updated_date) desc) row_num
    from {{ref('FlatFileAllOrdersReportByLastUpdate')}} 
    )
where row_num = 1

