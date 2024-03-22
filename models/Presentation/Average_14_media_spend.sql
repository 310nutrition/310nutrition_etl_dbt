With cte as
(
Select date, store_name, SUM(adspend) adspend
from {{ref("marketing_deepdive")}}
group by 1,2
order by 1 DESC
)

Select *, ROUND(Avg(adspend) OVER(PARTITION BY store_name ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),2) average_14_days
from cte
order by date DESC