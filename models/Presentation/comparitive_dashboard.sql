{{config(materialized="table")}}



with traffic as (SELECT platform_name,
{{store_name('store_name')}},
date,
sum(sessions) as total_traffic,
sum(converted_sessions) as converted_traffic,
sum(if(lower(Custom_Grouping)='bing paid',sessions,0)) as bing_paid_traffic,
sum(if(lower(Custom_Grouping)='facebook paid',sessions,0)) as facebook_paid_traffic,
sum(if(lower(Custom_Grouping)='google paid',sessions,0)) as google_paid_traffic,
sum(if(lower(Custom_Grouping)='klaviyo',sessions,0)) as klaviyo_traffic,
sum(if(lower(Custom_Grouping)='organic',sessions,0)) as organic_traffic,
sum(if(lower(Custom_Grouping)='other paid',sessions,0)) as other_paid_traffic,
sum(if(lower(Custom_Grouping)='referral',sessions,0)) as referral_traffic
FROM {{ref('custom_channel_data')}}
group by 1,2,3),

ad_data as (SELECT brand_name,
platform_name, 
store_name, 
date, 
sum(adspend) as total_adspend,
sum(if(lower(ad_channel)='bing',adspend,0)) as bing_adspend,
sum(if(lower(ad_channel)='facebook',adspend,0)) as facebook_adspend,
sum(if(lower(ad_channel)='google',adspend,0)) as google_adspend,
sum(if(lower(ad_channel)='amazon',adspend,0)) as amazon_adspend
FROM {{ref('marketing_deepdive')}}
where lower(ad_channel) != 'klaviyo'
group by 1,2,3,4),

cte5 as (select date, 
platform_name,
store_name,
  if(order_type = 'Order',order_id,null) order_id, 
  if(order_type = 'Order',customer_id,null) customer_id, 
  sum(coalesce(item_subtotal_price,0)+coalesce(item_shipping,0)+coalesce(item_taxes,0)-coalesce(item_discount,0)+coalesce(item_shipping_tax,0)-coalesce(item_refund_amount,0)-coalesce(item_shipping_discount,0)) as total_revenue,
  max(is_subscription) as is_subscription,
  if((customer_order_sequence = 1), "New Customer" , "Returning Customer") New_vs_return
from {{ref('custom_sales')}}
where store_name = 'United States'
group by 1,2,3,order_id,customer_id,customer_order_sequence),

cte6 as (
select * except(is_subscription, New_vs_return),  
 case
  when is_subscription = true and New_vs_return = 'Returning Customer' then 'Returning Subscriber'
  when is_subscription = FALSE and New_vs_return = 'New Customer' then 'New OTP'
  when is_subscription = False and New_vs_return = 'Returning Customer' then 'Returning OTP'
  when is_subscription = true and New_vs_return = 'New Customer' then 'New Subscriber'
  end as category
from cte5
),

cte7 as (select date, 
platform_name,
store_name,
category, 
count(distinct order_id) as orders,
count(distinct customer_id) as customers,
sum(total_revenue) as total_rev,

case when category = 'New OTP' then count(distinct order_id) end as NEW_OTP_Orders,
case when category = 'Returning OTP' then count(distinct order_id) end as Returning_OTP_Orders,

case when category = 'New OTP' then count(distinct customer_id) end as NEW_OTP_Customer,
case when category = 'Returning OTP' then count(distinct customer_id) end as Returning_OTP_Customer,

case when category = 'New OTP' then sum(total_revenue)  end as NEW_OTP_Revenue,
case when category = 'Returning OTP' then sum(total_revenue)  end as Returnign_OTP_Revenue,

case when category = 'New Subscriber' then count(distinct order_id) end as NEW_Subscriber_Orders,
case when category = 'Returning Subscriber' then count(distinct order_id) end as Returning_Subscriber_Orders,

case when category = 'New Subscriber' then count(distinct customer_id) end as NEW_Subscriber_Customer,
case when category = 'Returning Subscriber' then count(distinct customer_id) end as Returning_Subscriber_Customer,

case when category = 'New Subscriber' then sum(total_revenue)  end as NEW_Subscriber_Revenue,
case when category = 'Returning Subscriber' then sum(total_revenue)  end as Returning_Subscriber_Revenue

from cte6
group by 1,2,3,4),

sales as (select date, 
platform_name,
store_name,
  sum(orders) as total_orders,
  sum(customers) as total_customers,
  sum(total_rev) as total_revenue,
  
  sum(NEW_OTP_Orders) as NEW_OTP_Orders,
  sum(NEW_OTP_Revenue) as NEW_OTP_revenue,
  sum(NEW_OTP_Customer) as NEW_OTP_Customers,

  sum(new_Subscriber_Orders) as new_Subscriber_Orders,
  sum(new_Subscriber_revenue) as new_Subscriber_revenue,
  sum(new_Subscriber_Customer) as new_Subscriber_Customers,

  sum(Returning_OTP_Orders) as Returning_OTP_Orders,
  sum(Returnign_OTP_Revenue) as Returning_OTP_revenue,
  sum(Returning_OTP_Customer) as Returning_OTP_Customers,

  sum(Returning_Subscriber_Orders) as Returning_Subscriber_Orders,
  sum(Returning_Subscriber_revenue) as Returning_Subscriber_revenue,
  sum(Returning_Subscriber_Customer) as Returning_Subscriber_Customers,


from cte7
group by 1,2,3)



select 
    a.date,
    a.platform_name,
    a.store_name,
--Traffic Data
    COALESCE(total_traffic, 0) AS total_traffic,
    COALESCE(converted_traffic, 0) AS converted_traffic,
    COALESCE(bing_paid_traffic, 0) AS bing_paid_traffic,
    COALESCE(facebook_paid_traffic, 0) AS facebook_traffic,
    COALESCE(google_paid_traffic, 0) AS google_paid_traffic,
    COALESCE(klaviyo_traffic, 0) AS klaviyo_traffic,
    COALESCE(other_paid_traffic, 0) AS other_paid_traffic,
    COALESCE(organic_traffic, 0) AS organic_traffic,
    COALESCE(referral_traffic, 0) AS referral_traffic,
--Order Data
    COALESCE(total_revenue, 0) AS Total_revenue,
    COALESCE(NEW_OTP_revenue, 0) AS New_OTP_revenue,
    COALESCE(new_Subscriber_revenue, 0) AS New_Subscriber_revenue,
    COALESCE(Returning_OTP_revenue, 0) AS Returning_OTP_revenue,
    COALESCE(Returning_Subscriber_revenue, 0) AS Returning_Subscriber_revenue,

    COALESCE(total_orders, 0) AS total_orders,
    COALESCE(NEW_OTP_Orders, 0) AS New_OTP_Orders,
    COALESCE(new_Subscriber_Orders, 0) AS New_Subscriber_Orders,
    COALESCE(Returning_OTP_Orders, 0) AS Returning_OTP_Orders,
    COALESCE(Returning_Subscriber_Customers, 0) AS Returning_Subscriber_Customers,

    COALESCE(total_customers, 0) AS total_customers,
    COALESCE(NEW_OTP_Customers, 0) AS New_OTP_Customers,
    COALESCE(new_Subscriber_Customers, 0) AS New_Subscriber_Customers,
    COALESCE(Returning_OTP_Customers, 0) AS Returning_OTP_Customers,
    COALESCE(Returning_Subscriber_Orders, 0) AS Returning_Subscriber_Orders,
    
--Spent Data    
    COALESCE(total_adspend, 0) AS total_adspend,
    COALESCE(google_adspend, 0) AS google_adspend,
    COALESCE(facebook_adspend, 0) AS facebook_adspend,
    COALESCE(bing_adspend, 0) AS bing_adspend,
    COALESCE(amazon_adspend, 0) AS amazon_adspend
from sales a 
left join ad_data b on a.date = b.date and a.platform_name = b.platform_name and a.store_name = b.store_name
left join traffic c on a.date = c.date and a.platform_name = c.platform_name and a.store_name = c.store_name
order by 1 desc