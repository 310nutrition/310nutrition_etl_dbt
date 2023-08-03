-- Dependencies generated through pipeline script.
-- depends_on: {{ ref('fact_advertising_klaviyo') }}
-- depends_on: {{ ref('fact_advertising_bingads') }}
-- depends_on: {{ ref('fact_advertising_facebookads') }}
-- depends_on: {{ ref('fact_advertising_googleads') }}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX({{to_epoch_milliseconds('last_updated')}}) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
{{set_table_name_modelling('fact_advertising%')}} 
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{% for i in results_list %}
    select
    {{ dbt_utils.surrogate_key(['campaign_id','campaign_type'])}} AS campaign_key,
    {{ dbt_utils.surrogate_key(['adgroup_id','campaign_type'])}} AS adgroup_key,
    {{ dbt_utils.surrogate_key(['ad_id', 'ad_type']) }} AS ad_key,
    {{ dbt_utils.surrogate_key(['flow_id','ad_channel']) }} AS flow_key,
    {{ dbt_utils.surrogate_key(['brand'])}} AS brand_key,
    {{ dbt_utils.surrogate_key(['platform_name','store_name'])}} AS platform_key,
    {{ dbt_utils.surrogate_key(['product_id','sku','platform_name'])}} AS product_key,
    date,
    clicks,
    impressions,
    conversions,
    email_deliveries,
    email_opens,
    email_unsubscriptions,
    round((spend/exchange_currency_rate),2) as spend,
    round((sales/exchange_currency_rate),2) as sales,
    quantity as sold_quantity, 
    exchange_currency_code as currency_code,
    cast(null as int) as interactions,
    cast(null as decimal) as interaction_rate,
    cast(null as int) as engagements,
    cast(null as decimal) as engagement_rate,
    cast(null as int) as video_views,
    current_timestamp() as last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}}
    {% if is_incremental() %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    WHERE {{to_epoch_milliseconds('current_timestamp()')}}  >= {{max_loaded}}
    {% endif %}
    {% if not loop.last %} union all {% endif %}
{% endfor %}