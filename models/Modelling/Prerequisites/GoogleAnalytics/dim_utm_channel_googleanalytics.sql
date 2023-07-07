    {% if target.type =='bigquery' %}
    select
    distinct 
    source,
    medium,
    name as campaign,
    '' as keyword,
    '' as  content,
    'Google Analytics' as channel_name,
    event_timestamp/1000 as _daton_batch_runtime   --   Since event_timestamp is coming in microseconds 
    from {{ ref('GoogleAnalyticsEvents') }}
    {% else %}
    select
    null as source,
    null as medium,
    null as campaign,
    null as keyword,
    null as content,
    null as channel_name, 
    null as _daton_batch_runtime   
    {% endif %}
