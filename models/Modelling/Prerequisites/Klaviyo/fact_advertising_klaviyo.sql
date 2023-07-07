
with delivered_email as (
    select 
    brand,
    date,
    store,
    Campaign_Name,
    flow,
    count(profile_id) as email_deliveries,
    from {{ ref('KlaviyoReceivedEmail') }} 
    group by 1,2,3,4,5
    ),
    
opened_email as (
    select 
    brand,
    date(timestamp) as date,
    store,
    Campaign_Name,
    flow,
    count(profile_id) as email_opens,
    from {{ ref('KlaviyoOpenedEmail') }} 
    group by 1,2,3,4,5
    ),

unsubscribed_email as (
    select
    brand, 
    date(timestamp) as date,
    store,
    Campaign_Name,
    flow,
    count(profile_id) as email_unsubscriptions,
    from {{ ref('KlaviyoUnsubscribed') }} 
    group by 1,2,3,4,5
    ),

clicked_email as (
    select 
    brand,
    date(timestamp) as date,
    store,
    Campaign_Name,
    flow,
    count(profile_id) as clicks,
    from {{ ref('KlaviyoClickedEmail') }} 
    group by 1,2,3,4,5
    ),

attributed_orders as (
    select 
    brand,
    date(datetime) as date,
    store,
    campaigns.name as Campaign_Name,
    flows.name as flow,
    _currency_code,
    sum(_value) as adsales,
    sum(item_count) as quantity
    from {{ ref('KlaviyoPlacedOrder') }} orders
    left join (select distinct id, name from {{ ref('KlaviyoCampaigns') }}) campaigns 
    on orders._message = campaigns.id
    left join (select distinct id, name from {{ ref('KlaviyoFlows') }}) flows 
    on orders._flow = flows.id
    group by 1,2,3,4,5,6
    )

select 
delivered_email.brand,
{{ store_name('delivered_email.store') }},
campaigns.id as campaign_id,
delivered_email.Campaign_Name as campaign_name,
flows.id as flow_id,
delivered_email.flow as flow_name,
'' as adgroup_id, 
'' as adgroup_name,
'' as ad_id, 
'' as product_id,
'' as sku,
delivered_email.date,
{% if var('currency_conversion_flag') %}
case when exchange_rates.value is null then 1 else exchange_rates.value end as exchange_currency_rate,
case when exchange_rates.from_currency_code is null then attributed_orders._currency_code else exchange_rates.from_currency_code end as exchange_currency_code,
{% else %}
cast(1 as decimal) as exchange_currency_rate,
attributed_orders._currency_code as exchange_currency_code, 
{% endif %}
'Shopify' as platform_name,
'Klaviyo' as ad_channel,
'Email Marketing' as campaign_type,
trigger_type as flow_type,
sum(cast(clicks as numeric)) clicks,
sum(cast(null as numeric)) as impressions,
sum(cast(quantity as numeric)) conversions,
sum(cast(quantity as numeric)) quantity,
sum(cast(null as numeric)) as spend,
sum(cast(adsales as numeric)) as sales, 
sum(cast(email_deliveries as numeric)) email_deliveries,
sum(cast(email_opens as numeric)) email_opens,
sum(cast(email_unsubscriptions as numeric)) email_unsubscriptions
from delivered_email  
left join opened_email 
on delivered_email.date = opened_email.date 
and delivered_email.store = opened_email.store
and delivered_email.Campaign_Name = opened_email.Campaign_Name
and delivered_email.flow = opened_email.flow
left join unsubscribed_email 
on delivered_email.date = unsubscribed_email.date 
and delivered_email.store = unsubscribed_email.store
and delivered_email.Campaign_Name = unsubscribed_email.Campaign_Name
and delivered_email.flow = unsubscribed_email.flow
left join clicked_email 
on delivered_email.date = clicked_email.date 
and delivered_email.store = clicked_email.store
and delivered_email.Campaign_Name = clicked_email.Campaign_Name
and delivered_email.flow = clicked_email.flow
left join attributed_orders
on delivered_email.date = attributed_orders.date 
and delivered_email.store = attributed_orders.store
and delivered_email.Campaign_Name = attributed_orders.Campaign_Name
and delivered_email.flow = attributed_orders.flow
{% if var('currency_conversion_flag') %}
left join {{ref('ExchangeRates')}} exchange_rates 
on date(delivered_email.date) = exchange_rates.date 
and attributed_orders._currency_code = exchange_rates.to_currency_code                      
{% endif %}
left join (select distinct id, name from {{ ref('KlaviyoCampaigns') }}) campaigns 
on delivered_email.Campaign_Name = campaigns.name
left join (select distinct id, name, trigger_type from {{ ref('KlaviyoFlows') }}) flows 
on delivered_email.flow = flows.name
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18

