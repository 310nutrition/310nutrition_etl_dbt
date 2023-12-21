{% if var('profit_and_loss') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}


select 
brand_name,
c.platform_name,
store_name,
date,
amount_type,
transaction_type,
charge_type,
coalesce(product_id,'') as product_id,
coalesce(sku,'') as sku,
currency_code,
sum(amount) amount,
sum(absolute_amount) absolute_amount
from 
    (
    select 
    brand_key,
    platform_key,
    product_key,
    date,
    amount_type,
    transaction_type,
    charge_type,
    currency_code,
    amount,
    abs(amount) absolute_amount
    from {{ ref('fact_finances')}}

    union all 

    select 
    brand_key,
    platform_key,
    product_key,
    date,
    'Total Revenue' as amount_type,
    'Total Revenue' as transaction_type,
    'Total Revenue' as charge_type,
    currency_code,
    case when amount_type in ('Order Revenue', 'Refund Taxes', 'Refund Fees') then amount else 0 end as amount,
    case when amount_type in ('Order Revenue', 'Refund Taxes', 'Refund Fees') then abs(amount) else 0 end as amount
    from {{ ref('fact_finances')}}

    union all

    select 
    brand_key,
    platform_key,
    product_key,
    date,
    'Total Costs' as amount_type,
    'Total Costs' as transaction_type,
    'Total Costs' as charge_type,
    currency_code,
    case when amount_type in ('Service Fees', 'Order Taxes', 'Refund Revenue', 'Order Fees', 'Order Promotional Discount', 'Advertising Spend') then amount else 0 end as amount,
    case when amount_type in ('Service Fees', 'Order Taxes', 'Refund Revenue', 'Order Fees', 'Order Promotional Discount', 'Advertising Spend') then abs(amount) else 0 end as amount
    from {{ ref('fact_finances')}}

    union all

    select 
    brand_key,
    platform_key,
    product_key,
    date,
    'Profit' as amount_type,
    'Profit' as transaction_type,
    'Profit' as charge_type,
    currency_code,
    sum(amount) amount,
    sum(abs(amount)) absolute_amount
    from {{ ref('fact_finances')}}
    group by 1,2,3,4,5,6,7,8
    ) a
left join (select brand_key, brand_name from {{ref('dim_brand')}} where status = 'Active') b
on a.brand_key = b.brand_key
left join {{ ref('dim_platform')}} c
on a.platform_key = c.platform_key
left join (select product_key, product_id, product_name, sku from {{ref('dim_product')}} where status = 'Active') d
on a.product_key = d.product_key
where currency_code is not null
group by 1,2,3,4,5,6,7,8,9,10




