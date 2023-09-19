select * {{exclude()}} (row_num) from 
    (
    select 
    adgroupvideo.campaignId as campaign_id, 
    'Sponsored Brand Videos' as campaign_type, 
    campaignName as campaign_name, 
    coalesce(portfolioId,'') as portfolio_id,
    coalesce(name,'') as portfolio_name,
    'Amazon' as ad_channel, 
    campaignStatus as status,
    campaignBudget as budget, 
    campaignBudgetType as budget_type, 
    cast(null as string) as campaign_placement,
    cast(null as decimal) as bidding_amount, 
    cast(null as string) as bidding_strategy_type,
    date(RequestTime) as last_updated_date,
    row_number() over(partition by adgroupvideo.campaignId order by date(RequestTime) desc) row_num
    from {{ ref('SBAdGroupsVideoReport') }} adgroupvideo
    left join 
        (
        select distinct 
        campaign.portfolioId, 
        portfolio.name, 
        campaign.campaignId
        from {{ ref('SBCampaign') }} campaign
        left join {{ ref('SBPortfolio') }} portfolio
        on campaign.portfolioId = portfolio.portfolioId
        ) portfolio_map
    on adgroupvideo.campaignId = portfolio_map.campaignId
    )
where row_num = 1