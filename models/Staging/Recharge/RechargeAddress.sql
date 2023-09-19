
{% if var('RechargeAddress') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
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
{{set_table_name('%recharge%address')}}    
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
        customer_id,		
        payment_method_id,		
        address1,		
        address2,		
        city,		
        company,		
        country_code,		
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="created_at") }} as {{ dbt.type_timestamp() }}) as created_at,	
        first_name,		
        last_name,		
        phone,		
        province,		
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,		
        zip,		
        order_attributes,		
        order_note,		
        presentment_currency,		
        discounts,		
        cart_attributes,		
        note_attributes,		
        cart_note,		
        country,		
        created_at_dtm,		
        updated_at_dtm,		
        discount_id,		
        shipping_lines_conserved,		
        shipping_lines_override,		
        original_shipping_lines,	
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        DENSE_RANK() OVER (PARTITION BY id,payment_method_id order by {{daton_batch_runtime()}} desc) row_num
        from {{i}} 
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            --WHERE 1=1
            {% endif %}
        )
    where row_num =1 
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
