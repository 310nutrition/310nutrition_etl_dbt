{{config(materialized="table")}}



with cte1 as (
    select
    date,
    case
        when lower(sessionDefaultChannelGroup) like "%paid%" then "Paid Traffic"
        when lower(sessionDefaultChannelGroup) in ("sms","email") then "Paid Traffic"
        when lower(sessionDefaultChannelGroup) = "unassigned" then "Unattributed Traffic"
        else "Organic Traffic" end as traffic_source,
    totalusers,
    sessions, 
    round(sessions*sessionConversionRate,0) converted_sessions
    from {{ref('ga4_traffic_and_conversions')}}
),

cte2 as (select
    date,
    case
        when traffic_source = "Paid Traffic" then sum(totalusers) end as Paid_Traffic,
    case
        when traffic_source = "Organic Traffic" then sum(totalusers) end as Organic_Traffic,
    case
        when traffic_source = "Unattributed Traffic" then sum(totalusers) end as Unattributed_Traffic,
    sum(totalusers) as totalusers,
    sum(sessions) as  sessions,
    sum(converted_sessions) as converted_sessions
from cte1
group by 1,traffic_source),

traffic as (select 
  date,
  sum(paid_traffic) as paid_traffic,
  sum(organic_traffic) as organic_traffic,
  sum(unattributed_traffic) as unattributed_traffic,
  sum(totalusers) as total_traffic,
  sum(sessions) as total_sessions,
  sum(converted_sessions) as converted_sessions
from cte2
group by 1
order by 1 desc),

cte3 as (select date, ad_channel, sum(adspend) as adspend
from {{ref("marketing_deepdive")}}
where store_name = 'United States' and ad_channel in ('Google', 'Facebook', 'Bing')
group by 1,2),

cte4 as (select date,
    case
        when ad_channel = "Google" then sum(adspend) end as google_adspend,
    case
        when ad_channel = "Facebook" then sum(adspend) end as facebook_adspend,
    case
        when ad_channel = "Bing" then sum(adspend) end as bing_adspend,
    sum(adspend) as adspend
from cte3
group by 1,ad_channel),

ad_data as (
  select
    date,
    coalesce(sum(google_adspend),0) as google_adspend,
    coalesce(sum(facebook_adspend),0) as facebook_adspend,
    coalesce(sum(bing_adspend),0) as bing_adspend,
    coalesce(sum(adspend),0) as total_adspend
  from cte4
  group by 1
  order by 1 desc
),

cte5 as (select date, 
  if(order_type = 'Order',order_id,null) order_id, 
  if(order_type = 'Order',customer_id,null) customer_id, 
  sum(coalesce(item_subtotal_price,0)+coalesce(item_shipping,0)+coalesce(item_taxes,0)-coalesce(item_discount,0)+coalesce(item_shipping_tax,0)-coalesce(item_refund_amount,0)-coalesce(item_shipping_discount,0)) as total_revenue,
  max(is_subscription) as is_subscription,
  if((customer_order_sequence = 1), "New Customer" , "Returning Customer") New_vs_return
from `production_presentation.custom_sales`
where platform_name = 'Shopify' and store_name = 'United States'
group by 1,order_id,customer_id,customer_order_sequence),

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

cte7 as (select date, category, count(distinct order_id) as orders,
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
group by 1,2),

sales as (select date,
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
group by 1)



select 
    a.date,
    COALESCE(paid_traffic, 0) AS paid_traffic,
    COALESCE(organic_traffic, 0) AS organic_traffic,
    COALESCE(unattributed_traffic, 0) AS unattributed_traffic,
    COALESCE(total_traffic, 0) AS total_traffic,
    COALESCE(total_sessions, 0) AS total_sessions,
    COALESCE(converted_sessions, 0) AS converted_sessions,
    COALESCE(total_orders, 0) AS total_orders,
    COALESCE(total_customers, 0) AS total_customers,
    COALESCE(total_revenue, 0) AS total_revenue,
    COALESCE(NEW_OTP_Orders, 0) AS NEW_OTP_Orders,
    COALESCE(NEW_OTP_revenue, 0) AS NEW_OTP_revenue,
    COALESCE(NEW_OTP_Customers, 0) AS NEW_OTP_Customers,
    COALESCE(new_Subscriber_Orders, 0) AS new_Subscriber_Orders,
    COALESCE(new_Subscriber_revenue, 0) AS new_Subscriber_revenue,
    COALESCE(new_Subscriber_Customers, 0) AS new_Subscriber_Customers,
    COALESCE(Returning_OTP_Orders, 0) AS Returning_OTP_Orders,
    COALESCE(Returning_OTP_revenue, 0) AS Returning_OTP_revenue,
    COALESCE(Returning_OTP_Customers, 0) AS Returning_OTP_Customers,
    COALESCE(Returning_Subscriber_Orders, 0) AS Returning_Subscriber_Orders,
    COALESCE(Returning_Subscriber_revenue, 0) AS Returning_Subscriber_revenue,
    COALESCE(Returning_Subscriber_Customers, 0) AS Returning_Subscriber_Customers,
    COALESCE(google_adspend, 0) AS google_adspend,
    COALESCE(facebook_adspend, 0) AS facebook_adspend,
    COALESCE(bing_adspend, 0) AS bing_adspend,
    COALESCE(total_adspend, 0) AS total_adspend
from sales a 
left join ad_data b on a.date = b.date
left join traffic c on a.date = c.date
order by 1 desc