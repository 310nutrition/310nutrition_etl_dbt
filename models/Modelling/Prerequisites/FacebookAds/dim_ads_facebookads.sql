select * {{exclude()}} (row_num) from 
    (
    select 
    ad_id,
    'Facebook' as ad_channel,
    ad_name,
    'Facebook' as ad_type,
    'Facebook' as campaign_type,
    adset_id as adgroup_id,
    adset_name as adgroup_name,
    date(date_start) as last_updated_date,
    row_number() over(partition by ad_id order by date(date_start) desc) row_num
    from {{ ref('FacebookAdinsights') }}
    )
where row_num = 1