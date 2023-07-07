
{% if var('sales_target_gs_flag') %}
-- depends_on: {{ ref('SalesTarget') }}
{% endif %}

select ord.*,
{% if var('sales_target_gs_flag') %}
  year,
  month,
  revenue_target,
  orders_target 
{% else %}
  extract(year from CURRENT_DATE()) year,
  extract(month from CURRENT_DATE()) month,
  cast(null as decimal) as revenue_target,
  cast(null as int) as orders_target  
{% endif %} 

from (
select 
brand as brand_name,
cast(null as string) as type,
cast(null as string) as description,
_daton_batch_runtime
from {{ ref('FlatFileAllOrdersReportByLastUpdate') }}) ord

{% if var('sales_target_gs_flag') %}
  left join (select brand_name, year, month, revenue_target, orders_target from {{ ref('SalesTarget') }} 
  where lower(platform_name) = 'amazon') sales_gs
  on ord.brand_name = sales_gs.brand_name
  {% endif %}



