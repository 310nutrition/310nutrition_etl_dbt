{{config(materialized="table")}}


WITH cum_orders AS (
  SELECT
    brand_name,
    platform_name,
    store_name,
    date,
    customer_id,
    order_id,
    MIN(date) OVER (PARTITION BY customer_id ORDER BY date ASC) AS acquisition_date,
    DATE_TRUNC(MIN(date) OVER (PARTITION BY customer_id ORDER BY date ASC), MONTH) AS acquisition_month,
    DATE_DIFF(DATE_TRUNC(date, MONTH), DATE_TRUNC(MIN(date) OVER (PARTITION BY customer_id ORDER BY date ASC), MONTH), MONTH) AS elapsed_month,
    SUM(item_total_price) AS item_total_price,
    SUM(item_subtotal_price) AS item_subtotal_price
  FROM {{ref('customer')}}
  GROUP BY 1, 2, 3, 4, 5, 6
),

cohorts as (SELECT
  brand_name,
  platform_name,
  store_name,
  acquisition_month,
  elapsed_month,
  COUNT(DISTINCT customer_id) AS active_customers,
  SUM(item_total_price) AS sum_item_total_price,
  sum(SUM(item_total_price)) OVER (PARTITION BY brand_name,platform_name,store_name,acquisition_month ORDER BY elapsed_month) AS running_item_total_price,
  SUM(item_subtotal_price) AS sum_item_subtotal_price,
  sum(SUM(item_subtotal_price)) OVER (PARTITION BY brand_name,platform_name,store_name,acquisition_month ORDER BY elapsed_month) AS running_item_subtotal_price, 
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
sum_item_total_price,
running_item_total_price,
sum_item_subtotal_price,
running_item_subtotal_price,
b.monthly_adspend
from cohorts as a 
left join ad_spends as b
on a.acquisition_month = b.month and a.brand_name = b.brand_name and a.platform_name=b.platform_name and a.store_name = b.store_name
order by monthly_adspend desc),

LTVbyCAC as (select brand_name,
platform_name,
store_name,
acquisition_month,
elapsed_month,
acquired_customers,
active_customers,
running_item_total_price,
running_item_subtotal_price,
monthly_adspend,
round(safe_divide(monthly_adspend,acquired_customers),2) as CAC
from cohorts_spends)

select brand_name,
platform_name,
store_name,
acquisition_month,
elapsed_month,
acquired_customers,
active_customers,
running_item_total_price,
running_item_subtotal_price,
monthly_adspend,
CAC,
running_item_total_price - monthly_adspend as total_revenue_minus_spends,
running_item_subtotal_price - monthly_adspend as subtotal_revenue_minus_spends,
safe_divide(running_item_total_price,acquired_customers) as TLTV,
safe_divide(running_item_subtotal_price,acquired_customers) as STLTV,
safe_divide(running_item_total_price,acquired_customers) - CAC as TLTV_minus_cac,
safe_divide(running_item_subtotal_price,acquired_customers) - CAC as STLTV_minus_cac,
safe_divide(safe_divide(running_item_total_price,acquired_customers),CAC) as TLTV_by_CAC,
safe_divide(safe_divide(running_item_subtotal_price,acquired_customers),CAC) as STLTV_by_CAC
from LTVbyCAC