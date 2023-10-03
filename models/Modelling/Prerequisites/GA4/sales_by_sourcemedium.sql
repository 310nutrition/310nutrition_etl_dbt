{{config(materialized="table")}}

with cte1 as (select
    a.brand_name,
    a.platform_name, 
    a.store_name, 
    a.date, 
    a.order_id, 
    a.customer_id, 
    a.customer_order_sequence, 
    a.is_subscription, 
    a.customer_prev_order_date, 
    a.gross_sales * coalesce(b.google_convertion_ratio,1) as gross_sales, 
    a.customer_type,
    coalesce(b.Mapped_source,'Unattributed') as Mapped_source,
    case
        when is_subscription = 'True' and customer_type = 'Returning Customer' then 'Subscription'
        when customer_type = 'New Customer' then coalesce(b.Mapped_source,'Unattributed')
        when is_subscription = 'False' and customer_type = 'Returning Customer' then 'Returning'
    end as attribution_type
from {{ref("orders_table")}} a
left join {{ref("order_mapping")}} b
on a.customer_id = b.customer_id and a.order_id = b.order_id)

select 
       brand_name,
       platform_name,
       store_name,
       date,
       attribution_type,
       sum(gross_sales) as gross_sales
from cte1
group by 1,2,3,4,5

