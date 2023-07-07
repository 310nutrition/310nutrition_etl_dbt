select 
adGroupId as adgroup_id,
adGroupName as adgroup_name,
'Amazon' as ad_channel,
campaignId as campaign_id, 
'Sponsored Display' as campaign_type, 
campaignName as campaign_name,
_daton_batch_runtime from {{ ref('SDProductAdsReport') }}