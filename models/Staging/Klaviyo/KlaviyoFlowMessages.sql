
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
{{set_table_name('%klaviyo%flow_messages')}}  
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
        type,
        id,
        att.name,
        att.channel,
        content.subject,
        content.preview_text,
        content.from_email,
        content.from_name,
        {% if var('timezone_conversion_flag') %}
           datetime(DATETIME_ADD(cast(att.created as timestamp), INTERVAL {{hr}} HOUR )) as created_time,
        {% else %}
           datetime(timestamp(att.created)) as created_time,
        {% endif %}
        {% if var('timezone_conversion_flag') %}
           datetime(DATETIME_ADD(cast(att.updated as timestamp), INTERVAL {{hr}} HOUR )) as updated_time,
        {% else %}
           datetime(timestamp(att.updated)) as updated_time,
        {% endif %}
        {% if var('timezone_conversion_flag') %}
           date(DATETIME_ADD(cast(att.updated as timestamp), INTERVAL {{hr}} HOUR )) as updated_date,
        {% else %}
           date(timestamp(att.updated)) as updated_date,
        {% endif %}
        links.self,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        {% if var('timezone_conversion_flag') %}
           DATETIME_ADD(cast(att.updated as timestamp), INTERVAL {{hr}} HOUR ) as _edm_eff_strt_ts,
        {% else %}
           CAST(att.updated as timestamp) as _edm_eff_strt_ts,
        {% endif %}
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime,
        ROW_NUMBER() OVER (PARTITION BY id,subject ORDER BY _daton_batch_runtime DESC) AS rnk
        FROM {{i}} a
        LEFT JOIN UNNEST (a.attributes) AS att
        LEFT JOIN UNNEST (att.content) as content
        LEFT JOIN UNNEST (links) as links
    {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE _daton_batch_runtime  >= {{max_loaded}}
            {% endif %})
    WHERE rnk = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}

