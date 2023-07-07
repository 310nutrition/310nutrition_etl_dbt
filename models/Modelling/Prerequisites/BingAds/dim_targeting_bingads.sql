select * {{exclude()}} (row_num) from 
    (
    select
    'Keyword' as targeting_type,
    coalesce(cast(keywordId as string),'') targeting_id,
    coalesce(keyword,'') targeting_text,
    coalesce(searchquery,'') search_term,
    coalesce(bidmatchType,'') match_type,
    cast(replace(CostPerConversion,'',null) as numeric) as bid_amount,
    exchange_currency_code as currency, 
    'Shopify' as platform,
    date(TimePeriod) as last_updated_date,
    row_number() over(partition by keywordId, searchquery order by _daton_batch_runtime desc) row_num
    from {{ ref('BingSearchQueryPerformanceReport')}} a
    left join (select distinct campaignId, exchange_currency_code, exchange_currency_rate from {{ ref('BingAdPerformanceReport')}}) b
    on a.campaignId = b.campaignId
    ) search_term
where row_num = 1
