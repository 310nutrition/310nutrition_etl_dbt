select 
prodads.campaignId as campaign_id, 
'Sponsored Display' as campaign_type, 
campaignName as campaign_name, 
coalesce(portfolioId,'') as portfolio_id,
coalesce(name,'') as portfolio_name,
'Amazon' as ad_channel, 
cast(campaign_status as string) as status, 
cast(budget as decimal) as budget, 
cast(budgetType as string) as budget_type, 
cast(null as string) as campaign_placement,
cast(null as decimal) as bidding_amount, 
cast(null as string) as bidding_strategy_type,
_daton_batch_runtime
from {{ ref('SDProductAdsReport') }} prodads
left join 
    (
    select distinct 
    campaign.portfolioId,
    campaign.budgetType as budgetType,
    campaign.budget as budget,
    campaign.state as campaign_status,
    portfolio.name, 
    campaign.campaignId
    from {{ ref('SDCampaign') }} campaign
    left join {{ ref('SDPortfolio') }} portfolio
    on campaign.portfolioId = portfolio.portfolioId
    ) portfolio_map
on prodads.campaignId = portfolio_map.campaignId