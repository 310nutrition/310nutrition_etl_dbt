select main.* {{exclude()}}(amazon_order_id), coalesce(b.buyeremail,'') email
from (
select
ord.amazon_order_id,
shp.buyer_name as full_name,
ord.address_type,
shp.ship_address_1 as addr_line_1,
shp.ship_address_2 as addr_line_2,
shp.ship_city as city,
shp.ship_state as state,
shp.ship_country as country,
shp.ship_postal_code as postal_code,
shp.ship_phone_number as phone,
ord._daton_batch_runtime
from {{ ref('FlatFileAllOrdersReportByLastUpdate') }} ord
left join
 (select distinct amazon_order_id, coalesce(ship_address_1,'') as ship_address_1,
  coalesce(ship_address_2,'') as ship_address_2, coalesce(ship_city,'') as ship_city,
  coalesce(ship_state,'') as ship_state, coalesce(ship_country,'') as ship_country,
  coalesce(ship_postal_code,'') as ship_postal_code, coalesce(ship_phone_number,'') as ship_phone_number, coalesce(buyer_name,'') as buyer_name, coalesce(buyer_email,'') as buyer_email
from {{ ref('FBAAmazonFulfilledShipmentsReport') }}) shp
on ord.amazon_order_id=shp.amazon_order_id
) main
left join {{ ref('ListOrder') }} b
on main.amazon_order_id = b.amazonorderid
