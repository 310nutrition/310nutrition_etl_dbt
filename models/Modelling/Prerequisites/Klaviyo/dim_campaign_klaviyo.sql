select * {{exclude()}} (row_num) from 
    (
    select 
    id as campaign_id, 
    'Email Marketing' as campaign_type, 
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
    date(substring(updated,0,10)) as last_updated_date,
    row_number() over(partition by id order by date(substring(updated,0,10)) desc) as row_num 
    from {{ ref('KlaviyoCampaigns') }}
    )
where row_num = 1