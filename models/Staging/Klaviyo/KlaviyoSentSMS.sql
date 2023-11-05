--replace(replace(left(datetime,19),"T"," "),"Z",":00") added instead of datetime because string column has invalid format at certain rows

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
{{set_table_name('%klaviyo%sent_sms')}}   
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
        a.type,
        id,
        att.metric_id as metric_id,
        att.profile_id as profile_id,
        timestamp_millis(cast(att.timestamp as integer)) as timestamp,
        ev.Campaign_Name,
        ev.from_number,
        ev.from_phone_region,
        ev.to_number,
        ev.to_phone_region,
        ev.message_body,
        ev.message_type,
        ev.intent_1,
        ev.intent_1_confidence,
        ev.message,
        ev.cohort_message_send_cohort,
        ev.flow,
        ev.event_id,
        extra.message_id,
        extra.inbound_message_id,
        extra.from_state,
        extra.from_city,
        extra.from_country,
        attr.attributed_event_id,
        attr.send_ts,
        attr.message as attr_message,
        attr.flow as attr_flow,
        attr.variation,
        attr.experiment,
        {% if var('timezone_conversion_flag') %}
            datetime(DATETIME_ADD(timestamp(replace(replace(left(datetime,19),'T',' '),'Z',':00')), INTERVAL {{hr}} HOUR )) as datetime,
        {% else %}
            datetime(timestamp(replace(replace(left(datetime,19),'T',' '),'Z',':00')))  as datetime,
        {% endif %}
        {% if var('timezone_conversion_flag') %}
            date(DATETIME_ADD(timestamp(replace(replace(left(datetime,19),'T',' '),'Z',':00')), INTERVAL {{hr}} HOUR )) as date,
        {% else %}
            date(timestamp(replace(replace(left(datetime,19),'T',' '),'Z',':00')))  as date,
        {% endif %}
        uuid,
        links.self,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        {% if var('timezone_conversion_flag') %}
           DATETIME_ADD(cast(replace(replace(left(datetime,19),'T',' '),'Z',':00') as timestamp), INTERVAL {{hr}} HOUR ) as _edm_eff_strt_ts,
        {% else %}
           CAST(replace(replace(left(datetime,19),'T',' '),'Z',':00') as timestamp) as _edm_eff_strt_ts,
        {% endif %}
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _daton_batch_runtime DESC) AS rnk
        FROM {{i}} a
        LEFT JOIN UNNEST (a.attributes) AS att
        LEFT JOIN UNNEST (att.event_properties) AS ev
        LEFT JOIN UNNEST(ev.extra) as extra
        LEFT JOIN UNNEST(ev.attribution) as attr 
        LEFT JOIN UNNEST (links) as links
    {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE _daton_batch_runtime  >= {{max_loaded}}
            {% endif %})
    WHERE rnk = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}