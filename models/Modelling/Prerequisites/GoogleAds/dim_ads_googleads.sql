select 
'' as ad_id,
campaign_advertising_channel_type as ad_channel,
cast(null as string) as ad_name,
campaign_advertising_channel_type as ad_type,
(cast(ad_group_id as string)) as adgroup_id,
ad_group_name as adgroup_name,
_daton_batch_runtime 
from {{ ref('GoogleAdsShoppingPerformanceView') }}