select * {{exclude()}} (row_num)
from (
    select
    nullif(address_type, '') address_type,
    nullif(addr_line_1, '') addr_line_1,
    nullif(addr_line_2, '') addr_line_2,
    nullif(city, '') city,
    '' as district,
    nullif(state, '') state,
    nullif(country, '') country,
    nullif(postal_code, '') postal_code,
    last_updated_date,
    row_number() over(partition by address_type, addr_line_1, addr_line_2, city, state, district, postal_code  order by _daton_batch_runtime desc) row_num
    from 
        (
        select 
        distinct 
        company as address_type,
        address1 as addr_line_1,
        address2 as addr_line_2,
        city,
        cast(null as string) as district,
        province as state,
        country, 
        zip as postal_code,
        cast(null as date) last_updated_date,
        _daton_batch_runtime
        from {{ ref('ShopifyCustomerAddress') }}
        ) 
    ) 
where row_num = 1



        