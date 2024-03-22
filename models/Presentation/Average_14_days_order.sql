With cte as
(
Select date,store_name, COUNT(distinct order_id) as orders
from {{ref("custom_sales")}}
where lower(order_type) = 'order'
group by 1,2 
order by 1 DESC,2 DESC
)

Select *, ROUND(AVG(orders) OVER(PARTITION BY store_name ORDER BY Date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),0) average_14_day_orders
from cte
order by date DESC