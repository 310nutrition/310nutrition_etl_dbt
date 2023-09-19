select * {{exclude()}} (row_num) from 
    (
    select 
    adId as ad_id,
    'Amazon' as ad_channel,
    cast(null as string) as ad_name,
    'Sponsored Display' as ad_type,
    'Sponsored Display' as campaign_type,
    adGroupId as adgroup_id,
    adGroupName as adgroup_name,
    date(RequestTime) as last_updated_date,
    row_number() over(partition by adId order by date(RequestTime) desc) row_num 
    from {{ ref('SDProductAdsReport') }}
    )
where row_num = 1