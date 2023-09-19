select * {{exclude()}} (row_num) from 
    (
    select
    case when lower(matchType) in ('targeting_expression_predefined') then 'Automatic Targeting'
    when lower(matchType) in ('broad','phrase','exact') then 'Manual Keyword Targeting'
    else 'Others' end as targeting_type,
    coalesce(cast(keywordId as string),'') targeting_id,
    coalesce(keywordText,'') targeting_text,
    coalesce(query,'') search_term,
    coalesce(matchType,'') match_type,
    cast(keywordbid as numeric) as bid_amount,
    {{currency_code('countryname')}} ,
    'Amazon Seller Central' as platform,
    date(reportDate) as last_updated_date,
    row_number() over(partition by keywordId, query order by _daton_batch_runtime desc) row_num
    from {{ref('SBSearchTermKeywordsVideoReport')}} 

    union all

    select
    case when lower(targetingType) in ('targeting_expression_predefined') then 'Automatic Targeting'
    when lower(targetingType) in ('targeting_expression') and lower(targetingText) like 'category%' then 'Manual Product Targeting' 
    when lower(targetingType) in ('targeting_expression') and lower(targetingText) not like 'category%' then 'Manual Category Targeting'
    else 'Others' end as targeting_type,
    coalesce(cast(targetId as string),'') targeting_id,
    coalesce(targetingText,'') targeting_text,
    '' as search_term,
    coalesce(targetingType,'') match_type,
    cast(null as numeric) as bid_amount,
    {{currency_code('countryName')}}, 
    'Amazon Seller Central' as platform,
    date(reportDate) as last_updated_date,
    row_number() over(partition by targetId order by _daton_batch_runtime desc) row_num
    from {{ref('SBTargetVideoReport')}} 

    ) search_term
where row_num = 1