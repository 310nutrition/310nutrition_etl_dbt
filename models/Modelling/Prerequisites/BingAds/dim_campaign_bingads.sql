select * {{exclude()}} (row_num) from 
    (
    select 
    campaignId as campaign_id, 
    'Bing' as campaign_type, 
    campaignName as campaign_name, 
    AccountId as portfolio_id,
    AccountName as portfolio_name, 
    'Bing' as ad_channel, 
    campaignStatus as status,
    cast(null as numeric) as budget, 
    cast(null as string) as budget_type, 
    '' as campaign_placement,
    cast(null as decimal) as bidding_amount, 
    cast(null as string) as bidding_strategy_type,
    date(TimePeriod) as last_updated_date,
    row_number() over(partition by campaignId order by date(TimePeriod) desc) row_num
    from {{ ref('BingAdPerformanceReport') }}
    )
where row_num = 1