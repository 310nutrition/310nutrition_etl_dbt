select 
cast(campaign_id as string) campaign_id, 
campaign_advertising_channel_type as campaign_type, 
campaign_name,
'' as portfolio_id,
'' as portfolio_name, 
'Google' as ad_channel, 
cast(null as string) as status,
cast(null as numeric) as budget, 
cast(null as string) as budget_type, 
cast(null as string) as campaign_placement,
cast(null as decimal) as bidding_amount, 
cast(null as string) as bidding_strategy_type,
_daton_batch_runtime
from {{ ref('GoogleAdsCampaign') }}