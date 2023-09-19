{{config(materialized="table")}}


WITH cum_orders AS (
  SELECT
    brand_name,
    platform_name,
    store_name,
    date,
    customer_id,
    order_id,
    min(customer_first_order_date) AS acquisition_date,
    DATE_TRUNC(min(customer_first_order_date), MONTH) AS acquisition_month,
    DATE_DIFF(DATE_TRUNC(date, MONTH), min(customer_first_order_date), MONTH) AS elapsed_month,
    sum(coalesce(item_subtotal_price,0)) as subtotal_price,
    sum(coalesce(item_subtotal_price,0)) + sum(coalesce(item_taxes,0)) + sum(coalesce(item_shipping,0)) + sum(coalesce(item_shipping_tax,0)) - sum(coalesce(item_shipping_discount,0)) + sum(coalesce(item_giftwrap,0)) - sum(coalesce(item_discount,0)) - sum(coalesce(item_refund_amount,0)) as net_sales,
    --sum(coalesce(item_shipping,0)) as item_shipping,
    --sum(coalesce(item_shipping_tax,0)) as item_shipping_tax,
    --sum(coalesce(item_shipping_discount,0)) as item_shipping_discount,
    --sum(coalesce(item_giftwrap,0)) as item_giftwrap,
    --sum(coalesce(item_discount,0)) as item_discount,
    --sum(coalesce(item_refund_amount,0)) as item_refund_amount
  FROM {{ref('custom_sales')}}
  GROUP BY 1, 2, 3, 4, 5, 6
),

cohorts as (SELECT
  brand_name,
  platform_name,
  store_name,
  acquisition_month,
  elapsed_month,
  COUNT(DISTINCT customer_id) AS active_customers,
  sum(subtotal_price) as gross_sales,
  sum(SUM(subtotal_price)) OVER (PARTITION BY brand_name,platform_name,store_name,acquisition_month ORDER BY elapsed_month) AS running_gross_sales,
  SUM(net_sales) AS net_sales,
  sum(SUM(net_sales)) OVER (PARTITION BY brand_name,platform_name,store_name,acquisition_month ORDER BY elapsed_month) AS running_net_sales, 
FROM cum_orders
GROUP BY 1, 2, 3, 4, 5),

ad_spends as (SELECT brand_name,
platform_name,
store_name,
date_trunc(date,month) as month,
sum(adspend) as monthly_adspend 
FROM {{ref('marketing_overview')}}
group by 1,2,3,4),

cohorts_spends as (select a.brand_name,
a.platform_name,
a.store_name,
a.acquisition_month,
elapsed_month,
max(active_customers) over(partition by a.brand_name,a.platform_name,a.store_name,acquisition_month) as acquired_customers,
active_customers,
net_sales,
running_net_sales,
gross_sales,
running_gross_sales,
b.monthly_adspend
from cohorts as a 
left join ad_spends as b
on a.acquisition_month = b.month and a.brand_name = b.brand_name and a.platform_name=b.platform_name and a.store_name = b.store_name
order by monthly_adspend desc)

select brand_name,
platform_name,
store_name,
acquisition_month,
elapsed_month,
acquired_customers,
active_customers,
running_net_sales,
running_gross_sales,
monthly_adspend,
round(safe_divide(monthly_adspend,acquired_customers),2) as CAC
from cohorts_spends
