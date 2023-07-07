select 
adset_id as adgroup_id,
adset_name as adgroup_name,
'Facebook' as ad_channel,
campaign_id, 
'Facebook' as campaign_type, 
campaign_name,
_daton_batch_runtime 
from {{ ref('FacebookAdinsights') }}