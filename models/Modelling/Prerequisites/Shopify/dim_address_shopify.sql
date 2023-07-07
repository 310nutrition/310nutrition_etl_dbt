select 
concat(first_name,'',last_name) as full_name,
company as address_type,
address1 as addr_line_1,
address2 as addr_line_2,
city,
province as state,
country as country, 
zip as postal_code,
phone,
_daton_batch_runtime,
'' as email,
from {{ ref('ShopifyCustomerAddress') }}