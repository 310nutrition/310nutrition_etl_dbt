-- Dependencies generated through pipeline script.
-- depends_on: {{ ref('dim_customer_amazonseller') }}
-- depends_on: {{ ref('dim_customer_shopify') }}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX({{to_epoch_milliseconds('last_updated')}}) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
{{set_table_name_modelling('dim_customer%')}}
and lower(table_name) not like '%address%'
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}


select * {{exclude()}} (row_num, order_date) from (
select
customer_key,
customer_id,
email,
order_date,
last_order_date,
acquisition_date,
acquisition_channel,
effective_start_date,
effective_end_date,
last_updated,
_run_id,
row_number() over(partition by customer_key order by acquisition_date) row_num
from (
select *,
MAX(order_date) OVER (PARTITION BY customer_key) AS last_order_date,
MIN(order_date) OVER (PARTITION BY customer_key) AS acquisition_date
from (
{% for i in results_list %}
    select 
    {{ dbt_utils.surrogate_key(['customer_id']) }} AS customer_key,
    customer_id,
    email,
    order_date,
    acquisition_channel,
    last_updated_date as effective_start_date,
    cast(null as date) as effective_end_date,
    current_timestamp() as last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}} 
    {% if is_incremental() %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE {{to_epoch_milliseconds('current_timestamp()')}}  >= {{max_loaded}}
    {% endif %}   
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
))) where row_num = 1
