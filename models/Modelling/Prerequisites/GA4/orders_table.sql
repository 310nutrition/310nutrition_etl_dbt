{{config(materialized="table")}}

with cte1 as (select
    brand_name,
    platform_name,
    store_name,
    date,
    order_id,
    customer_id,
    customer_order_sequence,
    is_subscription,
    --customer_first_order_date,
    --customer_last_order_date,
    lag(date) over(partition by customer_id order by date , order_id) as customer_prev_order_date,
    /*case
        when customer_order_sequence > 1 and date_diff(customer_last_order_date,customer_first_order_date,day) > 180 then "New Customer"
        when customer_order_sequence = 1 then "New Customer"
        else "Returning Customer" end as customer_type,*/
    sum(coalesce(item_subtotal_price,0)) as gross_sales
from {{ref('custom_sales')}}
where platform_name = 'Shopify' and store_name = 'United States' and order_type = 'Order'
group by 1,2,3,4,5,6,7,8)

select *,
    case
        when customer_order_sequence > 1 and date_diff(date,coalesce(customer_prev_order_date,date),day) > 180 then "New Customer"
        when customer_order_sequence = 1 then "New Customer"
        else "Returning Customer" end as customer_type
from cte1 
where date >= '2023-01-01'