select * {{exclude()}} (row_num) from (
select *, 
row_number() over(partition by adgroup_id, campaign_type order by last_updated_date desc) row_num
from (
    select 
    cast(null as string) as adgroup_id,
    cast(null as string) as adgroup_name,
    'Google' as ad_channel,
    cast(campaign_id as string) campaign_id, 
    campaign_advertising_channel_type as campaign_type, 
    campaign_name,
    date(date) as last_updated_date
    from {{ ref('GoogleAdsCampaign') }}

    union all

    select 
    (cast(ad_group_id as string)) as adgroup_id,
    ad_group_name as adgroup_name,
    'Google' as ad_channel,
    cast(campaign_id as string) campaign_id, 
    campaign_advertising_channel_type as campaign_type, 
    campaign_name,
    date(date) as last_updated_date
    from {{ ref('GoogleAdsShoppingPerformanceView') }}
    ))
where row_num = 1