With cte as
(
Select date,store_name, 
SUM(coalesce(item_subtotal_price,0)+ coalesce(item_shipping,0) - coalesce(item_discount,0) -coalesce(item_shipping_discount,0)) gross_sales
from {{ref("custom_sales")}}
group by 1,2
order by 1 DESC
)



Select *, Round(Avg(gross_sales) OVER(PARTITION BY store_name ORDER BY Date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),2) average_14_days
from cte
order by Date DESC