select 
campaignId as campaign_id, 
'Bing' as campaign_type, 
campaignName as campaign_name, 
'' as portfolio_id,
'' as portfolio_name, 
'Bing' as ad_channel, 
campaignStatus as status,
cast(null as numeric) as budget, 
cast(null as string) as budget_type, 
'' as campaign_placement,
cast(null as decimal) as bidding_amount, 
cast(null as string) as bidding_strategy_type,
_daton_batch_runtime
from {{ ref('BingAdPerformanceReport') }}