
{% if var('recharge_flag') %}
-- depends_on: {{ ref('RechargeOrdersLineItemsProperties') }}
{% endif %}

{% if var('upscribe_flag') %}
-- depends_on: {{ ref('UpscribeSubscriptionItems') }}
{% endif %}
with order_level as(
    select ords.brand, ords.order_id, order_shipping_price , order_price,ord_shp_lns.shipping_discount,ord_shp_lns.discount_type,sum(cast(ords.total_discounts AS numeric)) as order_discount,cast(current_total_tax as numeric) as order_total_tax
    from {{ ref('ShopifyOrders') }} ords
    left join (select order_id,brand, store, sum(cast(line_items_quantity as numeric)) order_quantity, sum(CAST(line_items_price AS numeric)*line_items_quantity) order_price from {{ref('ShopifyOrdersLineItems')}} group by 1,2,3) ord_ln_itms
    on ords.order_id=ord_ln_itms.order_id and ords.brand = ord_ln_itms.brand and ords.store = ord_ln_itms.store
    left join (select order_id,brand,discount_type,sum(cast(shipping_lines_price as numeric)) order_shipping_price , case when discount_type = 'shipping' then sum(cast(discount_amount as numeric)) else 0 end as shipping_discount from {{ref('ShopifyOrdersShippingLines')}} group by 1,2,3) ord_shp_lns
    on ords.order_id=ord_shp_lns.order_id and ords.brand = ord_shp_lns.brand 
    group by 1,2,3,4,5,6,8
)
select
base.order_id,
brand,
platform_name,
base.store_name,
base.product_id, 
base.sku,
currency,
exchange_currency_code,
exchange_currency_rate,
date,
subscription_id,
transaction_type, 
order_channel,
reason,
customer_id,
ship_address_type,
ship_address_1,
ship_address_2,
ship_city,
ship_district,
ship_state,
ship_country,
ship_postal_code,
bill_address_type,
bill_address_1,
bill_address_2,
bill_city,
bill_district,
bill_state,
bill_country,
bill_postal_code,
quantity,
total_price,
subtotal_price,
total_tax, 
shipping_price, 
giftwrap_price, 
disc_alloc.item_discount,
shipping_discount
from
(select
ord.order_id,
ord.brand,
'Shopify' as platform_name,
{{store_name('store')}},
cast(ord.line_items_product_id as string) as product_id, 
ord.line_items_sku as sku,
currency,
exchange_currency_code,
exchange_currency_rate,
date(created_at) as date,
if(lower(ord.tags) like ("%subscription%"),"Subscription",null) as subscription_id,
{% if var('recharge_flag') %}
  --recharge.subscription_id,
{% elif var('upscribe_flag') %}
  upscribe.subscription_id,
{% else %}
  '' as subscription_id,
{% endif %}
'Order' as transaction_type, 
order_channel,
'' as reason,
customer_id,
shipping_address_company as ship_address_type,
shipping_address_address1 as ship_address_1,
shipping_address_address2 as ship_address_2,
shipping_address_city as ship_city,
cast(null as string) as ship_district,
shipping_address_province as ship_state,
shipping_address_country as ship_country,
shipping_address_zip as ship_postal_code,
billing_address_company as bill_address_type,
billing_address_address1 as bill_address_1,
billing_address_address2 as bill_address_2,
billing_address_city as bill_city,
cast(null as string) as bill_district,
billing_address_province as bill_state,
billing_address_country as bill_country,
billing_address_zip as bill_postal_code,
sum(line_items_quantity) quantity,
sum(CAST(line_items_price AS numeric)*line_items_quantity + ifnull(CAST(tax_lines_price AS numeric),0)) total_price,
sum(CAST(line_items_price AS numeric)*line_items_quantity) subtotal_price,
order_total_tax/COUNT(*) OVER (PARTITION BY ord.order_id) as total_tax,
order_level.order_shipping_price / COUNT(*) OVER (PARTITION BY ord.order_id) as shipping_price,
cast(null as numeric) as giftwrap_price, 
order_level.order_discount / COUNT(*) OVER (PARTITION BY ord.order_id) as item_discount,
order_level.shipping_discount /  COUNT(*) OVER (PARTITION BY ord.order_id) as shipping_discount,
from {{ ref('ShopifyOrdersLineItems') }} ord
left join order_level on ord.order_id = order_level.order_id and ord.brand = order_level.brand 
--left join (select order_id, line_items_product_id, line_items_sku, sum(presentment_money_amount) as presentment_money_amount from {{ ref('ShopifyOrdersDiscountAllocations') }} group by 1,2,3) disc_alloc
--on ord.order_id= disc_alloc.order_id and ord.line_items_sku = disc_alloc.line_items_sku and ord.line_items_product_id = disc_alloc.line_items_product_id
left join (select distinct customer_id, order_id from  {{ ref('ShopifyOrdersCustomer') }} where customer_id is not null) info
on ord.order_id = info.order_id

-- fetching the subscription ids in case of recharge orders
{% if var('recharge_flag') %}
  left join (
  select distinct 'Recharge' as order_channel, 
  external_order_id as order_id, 
  sku,
  line_items_title, 
  variant_title,
  case when purchase_item_type='subscription' then cast(purchase_item_id as string)
  end as subscription_id
  from {{ ref('RechargeOrdersLineItemsProperties') }}) recharge
  on ord.order_id = recharge.order_id and (ord.line_items_sku = recharge.sku or ( ord.line_items_title = recharge.line_items_title and ord.line_items_variant_title = recharge.variant_title))
  {% endif %}

-- fetching the subscription ids in case of upscribe orders
{% if var('upscribe_flag') %}
  left join (
  select distinct 'Upscribe' as order_channel, 
  cast(shopify_order_id as string) as order_id,
  items_sku as sku,
  subscription_id 
  from {{ ref('UpscribeSubscriptionItems') }}) upscribe
  on ord.order_id= upscribe.order_id and ord.line_items_sku = upscribe.sku
{% endif %}

left join (select order_id,line_items_product_id,line_items_sku,sum(CAST(tax_lines_price AS numeric)) tax_lines_price from {{ref('ShopifyOrdersLineItemsTaxLines')}} group by 1,2,3) tax
on ord.order_id= tax.order_id and ord.line_items_sku = tax.line_items_sku and ord.line_items_product_id = tax.line_items_product_id

-- left join (select order_id,discount_applications_code,discount_applications_title,discount_applications_description, row_number() over(partition by order_id order by _daton_batch_runtime) as index from {{ref('ShopifyOrdersDiscountApplications')}}) f
-- on a.order_id=f.order_id and a.discount_application_index = f.index

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,order_shipping_price,order_price,order_discount,order_level.shipping_discount,order_level.order_total_tax,ord.tags

UNION ALL

select 
cast(ref.order_id as string) as order_id,
brand,
'Shopify' as platform_name,
{{store_name('store')}},
cast(ref.line_item_product_id as string) as product_id, 
ref.line_item_sku as sku,
subtotal_set_presentment_currency_code as currency,
exchange_currency_code, 
exchange_currency_rate,
date(ref.created_at) as date,
{% if var('recharge_flag') %}
  recharge.subscription_id,
{% elif var('upscribe_flag') %}
  upscribe.subscription_id,
{% else %}
'' as subscription_id,
{% endif %}
'Return' as transaction_type, 
order_channel,
'' as reason,
customer_id,
shipping_address_company as ship_address_type,
shipping_address_address1 as ship_address_1,
shipping_address_address2 as ship_address_2,
shipping_address_city as ship_city,
cast(null as string) as ship_district,
shipping_address_province as ship_state,
shipping_address_country as ship_country,
shipping_address_zip as ship_postal_code,
billing_address_company as bill_address_type,
billing_address_address1 as bill_address_1,
billing_address_address2 as bill_address_2,
billing_address_city as bill_city,
cast(null as string) as bill_district,
billing_address_province as bill_state,
billing_address_country as bill_country,
billing_address_zip as bill_postal_code,
sum(line_item_quantity) as quantity,
(total_transaction_amt / COUNT(*) OVER (PARTITION BY ref.order_id) + ifnull(CAST(refund_line_items_total_tax AS numeric),0)) total_price,
sum(CAST(refund_line_items_subtotal AS numeric)) subtotal_price,
sum(cast(refund_line_items_total_tax as numeric)) total_tax,
cast(null as numeric) as shipping_price, 
cast(null as numeric) as giftwrap_price, 
cast('' as numeric) as item_discount,
cast(null as numeric) as shipping_discount,
from {{ ref('ShopifyRefundsLineItems')}} ref
left join (select t.order_id, sum(cast(transactions_amount as numeric)) as total_transaction_amt from `nutritiondatondw.dbt_3nutrition_stg_shopify.ShopifyRefundsTransactions` t group by 1 ) r on ref.order_id = r.order_id 
--left join (select order_id, line_items_product_id, line_items_sku, sum(presentment_money_amount) as presentment_money_amount from {{ ref('ShopifyOrdersDiscountAllocations') }} group by 1,2,3) disc_alloc
--on cast(ref.order_id as string) = disc_alloc.order_id and cast(ref.line_item_sku as string) = disc_alloc.line_items_sku and ref.line_item_product_id = disc_alloc.line_items_product_id
left join ( 
    select 
    distinct order_id, 
    shipping_address_company,
    shipping_address_address1, 
    shipping_address_address2, 
    shipping_address_city, 
    shipping_address_province, 
    shipping_address_country, 
    shipping_address_zip,
    billing_address_company,
    billing_address_address1, 
    billing_address_address2, 
    billing_address_city, 
    billing_address_province, 
    billing_address_country, 
    billing_address_zip
    from {{ ref('ShopifyOrdersAddresses') }}
    ) address
on cast(ref.order_id as string) = address.order_id

-- fetching the subscription ids in case of recharge orders
{% if var('recharge_flag') %}
  left join (
  select distinct 'Recharge' as order_channel, 
  external_order_id as order_id, 
  sku, 
  case when purchase_item_type='subscription' then cast(purchase_item_id as string)
  end as subscription_id
  from {{ ref('RechargeOrdersLineItemsProperties') }}) recharge
  on ref.refund_id = recharge.order_id and ref.line_item_sku = recharge.sku
{% endif %}

-- fetching the subscription ids in case of upscribe orders
{% if var('upscribe_flag') %}
  left join (
  select distinct 'Upscribe' as order_channel, 
  cast(shopify_order_id as string) as order_id,
  items_sku as sku,
  subscription_id 
  from {{ ref('UpscribeSubscriptionItems') }}) upscribe
  on ref.refund_id= upscribe.order_id and ref.line_item_sku = upscribe.sku
{% endif %} 

left join (select cast(refund_id as string) as order_id,line_item_sku as items_sku, sum(cast(tax_lines_price as numeric)) refund_total_tax from {{ref('ShopifyRefundLineItemsTax')}} group by 1,2) ref_tax
on cast(ref.refund_id as string)= ref_tax.order_id and ref.line_item_sku = ref_tax.items_sku
left join (select distinct customer_id, order_id from  {{ ref('ShopifyOrdersCustomer') }} where customer_id is not null) info
on cast(ref.order_id as string) = info.order_id

where created_at is not null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,total_transaction_amt,ref.order_id,refund_line_items_total_tax

union all

select
cast(order_adjustments_order_id as string) as order_id,
adj.brand,
'Shopify' as platform_name,
{{store_name('store')}},
cast(null as string) as product_id, 
cast(null as string) as sku,
amount_set_presentment_money_currency_code as currency,
exchange_currency_code,
exchange_currency_rate,
date(created_at) as date,
cast(null as string) as subscription_id,
'Adjustment' as transaction_type, 
cast(null as string) order_channel,
'' as reason,
cast(null as string) customer_id,
cast(null as string)as ship_address_type,
cast(null as string) as ship_address_1,
cast(null as string)as ship_address_2,
cast(null as string)as ship_city,
cast(null as string) as ship_district,
cast(null as string) as ship_state,
cast(null as string) as ship_country,
cast(null as string)as ship_postal_code,
cast(null as string) as bill_address_type,
cast(null as string) as bill_address_1,
cast(null as string)as bill_address_2,
cast(null as string) as bill_city,
cast(null as string) as bill_district,
cast(null as string) as bill_state,
cast(null as string) as bill_country,
cast(null as string) as bill_postal_code,
cast(null as numeric) quantity,
sum(cast(order_adjustments_amount as numeric)) as total_price,
sum(cast(order_adjustments_amount as numeric)) as subtotal_price,
cast(null as numeric) as total_tax,
cast(null as numeric)as shipping_price,
cast(null as numeric) as giftwrap_price, 
cast(null as numeric) as item_discount,
cast(null as numeric)as shipping_discount,
from {{ ref('ShopifyRefundsOrderAdjustments') }} adj
group by 1,2,3,4,7,8,9,10,12
) base 
left join (select order_id, cast(line_items_product_id as string) as product_id , line_items_sku as sku, sum(presentment_money_amount) as item_discount from {{ ref('ShopifyOrdersDiscountAllocations') }} group by 1,2,3) disc_alloc
on cast(base.order_id as string) = disc_alloc.order_id and cast(base.sku as string) = disc_alloc.sku and base.product_id = disc_alloc.product_id

