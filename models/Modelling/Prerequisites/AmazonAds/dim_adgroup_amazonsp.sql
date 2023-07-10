select * {{exclude()}} (row_num) from 
    (
    select 
    adGroupId as adgroup_id,
    adGroupName as adgroup_name,
    'Amazon' as ad_channel,
    campaignId as campaign_id, 
    'Sponsored Products' as campaign_type, 
    campaignName as campaign_name, 
    date(RequestTime) as last_updated_date,
    row_number() over(partition by adGroupId order by date(RequestTime) desc) row_num 
    from {{ ref('SPProductAdsReport') }}
    )
where row_num = 1