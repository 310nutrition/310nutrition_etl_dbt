select AdId as ad_id,
'Bing' as ad_channel,
AdTitle as ad_name,
AdType as ad_type,
AdgroupId as adgroup_id,
AdgroupName as adgroup_name,
_daton_batch_runtime from {{ ref('BingAdPerformanceReport') }}