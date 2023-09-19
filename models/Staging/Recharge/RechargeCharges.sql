
{% if var('RechargeCharges') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
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
{{set_table_name('%recharge%charges')}}    
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

        {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
        {% else %}
            {% set hr = 0 %}
        {% endif %}

    SELECT * {{exclude()}} (row_num)
    From (
        select
        '{{brand}}' as brand,
        '{{store}}' as store,
        id,		
        address_id,		
        billing_address,		
        client_details,		
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="created_at") }} as {{ dbt.type_timestamp() }}) as created_at,	
        currency,		
        customer,		
        external_order_id,		
        external_transaction_id,		
        has_uncommitted_changes,
        line_items.purchase_item_id	as line_items_purchase_item_id,
        line_items.external_product_id as line_items_external_product_id,			
        line_items.external_variant_id as line_items_external_variant_id,				
        line_items.grams as line_items_grams,		
        line_items.images as line_items_images,			
        line_items.properties as line_items_properties,			
        line_items.purchase_item_type as line_items_purchase_item_type,		
        line_items.quantity as line_items_quantity,		
        line_items.sku as line_items_sku,		
        line_items.tax_due as line_items_tax_due,		
        line_items.taxable as line_items_taxable,		
        line_items.taxable_amount as line_items_taxable_amount,		
        line_items.title as line_items_title,		
        line_items.total_price as line_items_total_price,		
        line_items.unit_price as line_items_unit_price,		
        line_items.unit_price_includes_tax as line_items_unit_price_includes_tax,		
        line_items.variant_title as line_items_variant_title,		
        line_items.original_price as line_items_original_price,		
        line_items.price as line_items_price,		
        line_items.shopify_product_id as line_items_shopify_product_id,		
        line_items.shopify_variant_id as line_items_shopify_variant_id,		
        line_items.subscription_id as line_items_subscription_id,		
        line_items.type as line_items_type,		
        line_items.vendor as line_items_vendor,		
        line_items.shopify_product_id_st as line_items_shopify_product_id_st,		
        line_items.shopify_variant_id_st as line_items_shopify_variant_id_st,	
        note,		
        order_attributes,		
        orders_count,		
        payment_processor,		
        processed_at,		
        scheduled_at,		
        shipping_address,		
        shipping_lines,		
        status,		
        a.subtotal_price,		
        tags,		
        a.taxable,		
        a.total_discounts,		
        total_line_items_price,		
        a.total_price,		
        total_refunds,		
        a.total_tax,		
        total_weight_grams,		
        a.type,		
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,		
        discounts,		
        analytics_data,		
        error,		
        error_type,		
        retry_date,		
        charge_attempts,		
        last_charge_attempt,		
        note_attributes,		
        customer_hash,		
        customer_id,		
        email,		
        first_name,		
        has_uncommited_changes,		
        last_name,		
        processor_name,		
        requires_shipping,		
        shipments_count,		
        shopify_order_id,		
        tax_lines,		
        total_weight,		
        transaction_id,		
        created_at_dtm,		
        processed_at_dtm,		
        scheduled_at_dtm,		
        updated_at_dtm,		
        discount_codes,		
        last_charge_attempt_date,		
        number_times_tried,		
        external_variant_not_found,		
        total_duties,		
        retry_date_dtm,		
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then a.currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            a.currency as exchange_currency_code, 
        {% endif %}					
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        DENSE_RANK() OVER (PARTITION BY id order by a.{{daton_batch_runtime()}} desc) row_num
        from {{i}} a
            {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
            {% endif %}
            {{unnesting("line_items")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            --WHERE 1=1
            {% endif %}
        )
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
