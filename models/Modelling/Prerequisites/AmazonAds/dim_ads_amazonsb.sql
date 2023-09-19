select * {{exclude()}} (row_num) from 
    (
    select 
    cast(null as string) as ad_id,
    'Amazon' as ad_channel,
    cast(null as string) as ad_name,
    'Sponsored Brands' as ad_type,
    'Sponsored Brands' as campaign_type,
    adGroupId as adgroup_id,
    adGroupName as adgroup_name,
    date(RequestTime) as last_updated_date,
    row_number() over(order by date(RequestTime) desc) row_num 
    from {{ ref('SBAdGroupsReport') }}
    )
where row_num = 1