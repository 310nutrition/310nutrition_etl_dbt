select 
adId as ad_id,
'Amazon' as ad_channel,
cast(null as string) as ad_name,
'Sponsored Products' as ad_type,
adGroupId as adgroup_id,
adGroupName as adgroup_name,
_daton_batch_runtime 
from {{ ref('SPProductAdsReport') }}