select * {{exclude()}} (row_num) from 
    (
    select 
    AdId as ad_id,
    'Bing' as ad_channel,
    AdTitle as ad_name,
    AdType as ad_type,
    'Bing' as campaign_type, 
    AdgroupId as adgroup_id,
    AdgroupName as adgroup_name,
    date(TimePeriod) as last_updated_date,
    row_number() over(partition by AdId order by date(TimePeriod) desc) row_num
    from {{ ref('BingAdPerformanceReport') }}
    )
where row_num = 1