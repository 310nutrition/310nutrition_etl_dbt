select * {{exclude()}} (row_num) from 
    (
    select 
    cast(null as string) as adgroup_id,
    cast(null as string) as adgroup_name,
    'Klaviyo' as ad_channel,
    id as campaign_id, 
    'Email Marketing' as campaign_type, 
    name as campaign_name,
    date(substring(updated,0,10)) as last_updated_date,
    row_number() over(order by date(substring(updated,0,10)) desc) row_num
    from {{ ref('KlaviyoCampaigns') }}
    )
where row_num = 1