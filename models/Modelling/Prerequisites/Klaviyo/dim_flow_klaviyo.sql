select * {{exclude()}} (row_num) from 
    (
    select
    id as flow_id, 
    trigger_type as flow_type, 
    name as flow_name, 
    'Klaviyo' as ad_channel, 
    status,
    date(updated_date) as last_updated_date,
    row_number() over(partition by id order by date(updated_date) desc) row_num
    from {{ ref('KlaviyoFlows')}} 
    ) search_term
where row_num = 1