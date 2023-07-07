select 
AdgroupId as adgroup_id,
AdgroupName as adgroup_name,
'Bing' as ad_channel,
campaignId as campaign_id, 
'Bing' as campaign_type, 
campaignName as campaign_name,
_daton_batch_runtime 
from {{ ref('BingAdPerformanceReport') }}