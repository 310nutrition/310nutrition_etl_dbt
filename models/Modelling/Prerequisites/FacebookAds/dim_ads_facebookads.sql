select 
ad_id,
'Facebook' as ad_channel,
ad_name,
'Facebook' as ad_type,
adset_id as adgroup_id,
adset_name as adgroup_name,
_daton_batch_runtime
from {{ ref('FacebookAdinsights') }}