{% if target.type =='bigquery' %}

-- GA4 code follows. Comment lines 3-7 if GA4 is not in place
select 
event_name,
string_value as landing_page_path,
event_timestamp as _daton_batch_runtime
from {{ref('GoogleAnalyticsEventsEventParams')}}
where key = 'page_location'

{% else %}

select 
null as event_name,
null as landing_page_path,
null as _daton_batch_runtime

{% endif %}
