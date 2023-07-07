select 
id as campaign_id, 
campaign_type, 
name as campaign_name, 
'' as portfolio_id,
'' as portfolio_name, 
'Klaviyo' as ad_channel, 
status,
cast(null as numeric) as budget, 
cast(null as string) as budget_type, 
cast(null as string) as campaign_placement,
cast(null as decimal) as bidding_amount, 
cast(null as string) as bidding_strategy_type,
_daton_batch_runtime
from {{ ref('KlaviyoCampaigns') }}