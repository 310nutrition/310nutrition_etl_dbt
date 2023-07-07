select 
campaign_id, 
'Facebook' as campaign_type, 
campaign_name,
'' as portfolio_id,
'' as portfolio_name,  
'Facebook' as ad_channel, 
'' as status,
cast(null as numeric) as budget, 
cast(null as string) as budget_type, 
'' as campaign_placement,
cast(null as decimal) as bidding_amount, 
cast(null as string) as bidding_strategy_type,
_daton_batch_runtime,
row_number() over(partition by campaign_id, campaign_name order by _daton_batch_runtime desc) as row_num 
from {{ ref('FacebookAdinsights') }}