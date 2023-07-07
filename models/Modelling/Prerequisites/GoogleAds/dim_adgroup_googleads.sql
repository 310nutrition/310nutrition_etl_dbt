select 
(cast(ad_group_id as string)) as adgroup_id,
ad_group_name as adgroup_name,
'Google' as ad_channel,
cast(campaign_id as string) campaign_id, 
campaign_advertising_channel_type as campaign_type, 
campaign_name,
_daton_batch_runtime 
from {{ ref('GoogleAdsShoppingPerformanceView') }}