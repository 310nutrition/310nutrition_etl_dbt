
{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}


{% set table_name_query %}
{{set_table_name('%stayai_orders')}} 
{% endset %}  

{% set results = run_query(table_name_query) %}
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
{% set results_list = [] %}
{% set tables_lowercase_list = [] %}
{% endif %}

{% for i in results_list %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours')%}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

    SELECT * {{exclude()}} (row_num)
    FROM (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        REGEXP_REPLACE(orderid, '[^0-9]', '') as order_id, 
        cast(ordername as string) as order_name,
        cast(customerid as string) as customer_id,
        REGEXP_REPLACE(subscriptionid, '[^0-9]', '') as subscription_id,
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="createdat") }} as {{ dbt.type_timestamp() }})  as createdat,
        fulfillmentstatus as fulfillment_status,
        currency,
        totalPrice,	
        cartDiscountAmount,		
        currentTotalTax	,		
        totalShippingPrice,	
        tags,		
        REGEXP_REPLACE(lineItems.lineId , '[^0-9]', '') as line_item_id,
        lineItems.productTitle as line_item_product_title	,		
        REGEXP_REPLACE(lineItems.shopifyProductId, '[^0-9]', '') as line_item_shopify_product_id,		
        REGEXP_REPLACE(lineItems.shopifyVariantId, '[^0-9]', '') as line_item_shopify_variant_id	,		
        lineItems.sku as line_item_sku,		
        lineItems.quantity	as line_item_quantity,		
        lineItems.subtotalPrice	as line_item_subtotal_price,		
        lineItems.variantTitle	as line_item_variant_title,		
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="updatedat") }} as {{ dbt.type_timestamp() }})  as updatedat,
        {% if var('currency_conversion_flag') %}
            case when b.value is null then 1 else b.value end as exchange_currency_rate,
            case when b.from_currency_code is null then currency else b.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            currency as exchange_currency_code,
        {% endif %}
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        DENSE_RANK() OVER (PARTITION BY orderid, lineItems.sku order by a.{{daton_batch_runtime()}} desc, subscriptionid desc) row_num
            from {{i}} a
                {% if var('currency_conversion_flag') %}
                    left join {{ref('ExchangeRates')}} b on date(createdat) = b.date and currency = b.to_currency_code
                {% endif %}
                {{unnesting("lineItems")}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}

        )
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
