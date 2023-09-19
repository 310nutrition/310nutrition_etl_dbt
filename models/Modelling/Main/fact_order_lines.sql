-- Dependencies generated through pipeline script.
-- depends_on: {{ ref('fact_order_lines_amazonseller') }}
-- depends_on: {{ ref('fact_order_lines_shopify') }}

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
{{set_table_name_modelling('fact_order_lines%')}} 
and lower(table_name) not like 'fact_order_lines_unicom%' 
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{% for i in results_list %}
    select 
    {{ dbt_utils.surrogate_key(['order_id','platform_name']) }} AS order_key,
    {{ dbt_utils.surrogate_key(['platform_name','store_name']) }} AS platform_key,
    {{ dbt_utils.surrogate_key(['brand']) }} AS brand_key,
    {{ dbt_utils.surrogate_key(['product_id', 'sku','platform_name']) }} AS product_key,
    {{ dbt_utils.surrogate_key(['customer_id']) }} AS customer_key,
    {{ dbt_utils.surrogate_key(['subscription_id','sku']) }} AS subscription_key,
    {{ dbt_utils.surrogate_key(['ship_address_type','ship_address_1','ship_address_2','ship_city','ship_district','ship_state','ship_country','ship_postal_code']) }} AS shipping_address_key,
    {{ dbt_utils.surrogate_key(['bill_address_type','bill_address_1','bill_address_2','bill_city','bill_district','bill_state','bill_country','bill_postal_code']) }} AS billing_address_key,
    date,
    customer_id,
    subscription_id,
    order_id,
    case when product_id is null then '0' else product_id end as product_id,
    sku,
    transaction_type,
    reason,
    quantity,
    case when quantity != 0 then round((subtotal_price/exchange_currency_rate),2)/quantity else null end as unit_price,
    round((total_price/exchange_currency_rate),2) as item_total_price,
    round((subtotal_price/exchange_currency_rate),2) as item_subtotal_price,
    round((total_tax/exchange_currency_rate),2) as item_total_tax,
    round((shipping_price/exchange_currency_rate),2) as item_shipping_price,
    round((giftwrap_price/exchange_currency_rate),2) as item_giftwrap_price,
    round((item_discount/exchange_currency_rate),2) as item_discount,
    round((shipping_discount/exchange_currency_rate),2) as item_shipping_discount,
    exchange_currency_code as currency_code,
    ship_city,
    ship_state,
    ship_country,
    current_timestamp() as last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}}
    {% if is_incremental() %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where date  >= (SELECT MAX(date) FROM {{ this }})
    {% endif %}
    {% if not loop.last %} union all {% endif %}
{% endfor %}