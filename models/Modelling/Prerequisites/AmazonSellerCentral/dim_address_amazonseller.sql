select * {{exclude()}}(amazon_order_id, row_num)
from (
    select
    ord.amazon_order_id,
    nullif(ord.address_type, '') address_type,
    nullif(shp.addr_line_1, '') addr_line_1,
    nullif(shp.addr_line_2, '') addr_line_2,
    nullif(shp.city, '') city,
    '' as district,
    nullif(shp.state, '') state,
    nullif(shp.country, '') country,
    nullif(shp.postal_code, '') postal_code,
    date(ord.last_updated_date) last_updated_date,
    row_number() over(partition by ord.address_type, shp.addr_line_1, shp.addr_line_2, shp.city, shp.state, shp.country, shp.postal_code  order by date(ord.last_updated_date)) row_num
    from {{ ref('FlatFileAllOrdersReportByLastUpdate') }} ord
    left join
    (
    select 
    distinct 
    amazon_order_id, 
    ship_address_1 as addr_line_1,
    ship_address_2 as addr_line_2,
    ship_city as city,
    ship_state as state,
    ship_country as country,
    ship_postal_code as postal_code
    from {{ ref('FBAAmazonFulfilledShipmentsReport') }}
    
    union all

    select 
    distinct amazon_order_id, 
    bill_address_1 as addr_line_1,
    ship_address_2 as addr_line_2,
    ship_city as city,
    ship_state as state,
    ship_country as country,
    ship_postal_code as postal_code
    from {{ ref('FBAAmazonFulfilledShipmentsReport') }}
    ) shp
    on ord.amazon_order_id=shp.amazon_order_id
    ) 
where row_num = 1
