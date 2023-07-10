-- Dependencies generated through pipeline script.
-- depends_on: {{ ref('fact_orders_amazonseller') }}
-- depends_on: {{ ref('fact_orders_shopify') }}

    
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
{{set_table_name_modelling('fact_orders_%')}} and lower(table_name) not like 'fact_orders_unicom%'
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
        {{ dbt_utils.surrogate_key(['customer_id']) }} AS customer_key,
        date,
        transaction_type,
        quantity,
        round((total_price/exchange_currency_rate),2) as total_price,
        round((subtotal_price/exchange_currency_rate),2) as subtotal_price,
        round((total_tax/exchange_currency_rate),2) as total_tax,
        round((shipping_price/exchange_currency_rate),2) as shipping_price,
        round((giftwrap_price/exchange_currency_rate),2) as giftwrap_price,
        round((order_discount/exchange_currency_rate),2) as order_discount,
        round((shipping_discount/exchange_currency_rate),2) as shipping_discount,
        exchange_currency_code as currency_code,
        is_cancelled,
        current_timestamp() as last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{to_epoch_milliseconds('current_timestamp()')}}  >= {{max_loaded}}
            {% endif %}
    {% if not loop.last %} union all {% endif %}
{% endfor %}