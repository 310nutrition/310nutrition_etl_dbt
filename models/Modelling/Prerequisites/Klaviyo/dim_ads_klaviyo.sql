select * {{exclude()}} (row_num) from 
    (
    select 
    cast(null as string) ad_id,
    'Klaviyo' as ad_channel,
    cast(null as string) ad_name,
    'Email Marketing' as ad_type,
    'Email Marketing' as campaign_type,
    cast(null as string) as adgroup_id,
    cast(null as string) as adgroup_name,
    date(substring(updated,0,10)) as last_updated_date,
    row_number() over(order by date(substring(updated,0,10)) desc) row_num
    from {{ ref('KlaviyoCampaigns') }}
    )
where row_num = 1