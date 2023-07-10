select * {{exclude()}} (row_num) from 
    (
    select 
    AdgroupId as adgroup_id,
    AdgroupName as adgroup_name,
    'Bing' as ad_channel,
    campaignId as campaign_id, 
    'Bing' as campaign_type, 
    campaignName as campaign_name,
    date(TimePeriod) as last_updated_date,
    row_number() over(partition by AdgroupId order by date(TimePeriod) desc) row_num
    from {{ ref('BingAdPerformanceReport') }}
    )
where row_num = 1

