-- depends_on: {{ref('ExchangeRates')}}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime)-2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}


{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
{{set_table_name('%klaviyo%placed_order')}}   
{% endset %} 

{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}


{% if var('timezone_conversion_flag') %}
    {% set hr = var('timezone_conversion_hours') %}
{% endif %}

{% for i in results_list %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
            {% set id =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
        {% else %}
            {% set id = var('default_storename') %}
        {% endif %}

    SELECT * EXCEPT(rnk)
    FROM (
        SELECT
        '{{brand}}' as brand,
        '{{id}}' as store,
        event_properties._extra,
        _attribution._attributed_event_id,
        _attribution._send_ts,
        _attribution._message,
        _attribution._flow,
        _attribution._variation,
        _attribution._group_ids,
        _attribution._experiment,
        _attribution._attributed_channel,
        event_properties.items,
        event_properties.collections,
        event_properties.item_count,
        event_properties.tags,
        event_properties.total_discounts,
        event_properties.source_name,
        {% if var('currency_conversion_flag') %}
        case when c.value is null then 1 else c.value end as exchange_currency_rate,
        case when c.from_currency_code is null then event_properties._currency_code else c.from_currency_code end as exchange_currency_code,
        {% else %}
        cast(1 as decimal) as exchange_currency_rate,
        event_properties._currency_code as exchange_currency_code, 
        {% endif %} 
        event_properties._currency_code,
        event_properties._event_id,
        event_properties._value,
        event_properties.shippingrate,
        event_properties.discount_codes,
        attributes.metric_id,
        attributes.profile_id,
        attributes.timestamp,
        cast(attributes.datetime as timestamp) datetime,
        attributes.uuid,
        links.self,
        type,
        id,
        a._daton_user_id,
        a._daton_batch_runtime,
        a._daton_batch_id,
        unix_micros(current_timestamp()) as _edm_runtime,
        DENSE_RANK() OVER (PARTITION BY a.id, _attribution._attributed_event_id ORDER BY a._daton_batch_runtime DESC) AS rnk
        FROM {{i}} a
        LEFT JOIN UNNEST (attributes) AS attributes
        LEFT JOIN UNNEST (attributes.event_properties) AS event_properties
        LEFT JOIN UNNEST (event_properties._attribution) as _attribution
        LEFT JOIN UNNEST (links) as links
        {% if var('currency_conversion_flag') %}
        left join {{ref('ExchangeRates')}} c on date(cast(attributes.datetime as timestamp)) = c.date 
        and event_properties._currency_code = c.to_currency_code                      
        {% endif %}
        
        
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a._daton_batch_runtime  >= {{max_loaded}}
            {% endif %})
    WHERE rnk = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}

