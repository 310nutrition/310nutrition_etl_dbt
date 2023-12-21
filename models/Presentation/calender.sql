{% if var('calender') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

select Date,
extract(month from Date) Month,
extract(isoweek from Date) week,
FORMAT_DATE('%Y-%W', CAST(date(timestamp_trunc(date,week(Sunday))) as date)) as dateweek, 
format_date('%b',date) Monthname,
substr(cast(Date as STRING),1,7) as Year_Month,
FORMAT_DATE('%Y-%Q', date) datequarter,
extract(year from Date) Year, 
CAST(date(timestamp_trunc(Date,week(Sunday))) as date) as Week_Start_Date,
CAST(date(timestamp_trunc(Date,month)) as date) as Month_Start_Date,
('US') System
from UNNEST (GENERATE_DATE_ARRAY((SELECT min(date) from {{ ref('sales_overview') }}),CURRENT_DATE(), INTERVAL 1 DAY)) as Date