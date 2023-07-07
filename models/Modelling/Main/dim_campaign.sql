-- Dependencies generated through pipeline script.
-- depends_on: {{ ref('dim_campaign_klaviyo') }}
-- depends_on: {{ ref('dim_campaign_amazonsbv') }}
-- depends_on: {{ ref('dim_campaign_amazonsp') }}
-- depends_on: {{ ref('dim_campaign_amazonsd') }}
-- depends_on: {{ ref('dim_campaign_amazonsb') }}
-- depends_on: {{ ref('dim_campaign_bingads') }}
-- depends_on: {{ ref('dim_campaign_facebookads') }}
-- depends_on: {{ ref('dim_campaign_googleads') }}

      
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
{{set_table_name_modelling('dim_campaign%')}}
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{% for i in results_list %}

    select * {{exclude()}} (row_num) from (
    select 
    {{ dbt_utils.surrogate_key(['campaign_id', 'campaign_type', 'campaign_name', 'ad_channel']) }} AS campaign_key, 
    campaign_id, 
    campaign_type, 
    campaign_name,
    portfolio_id,
    portfolio_name, 
    coalesce(ad_channel,'') ad_channel, 
    status,
    budget, 
    budget_type, 
    campaign_placement,
    bidding_amount, 
    bidding_strategy_type,
    {{from_epoch_milliseconds()}} as effective_start_date,
    cast(null as date) as effective_end_date,
    current_timestamp() as last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
    row_number() over(partition by campaign_id, campaign_name order by _daton_batch_runtime desc) row_num
        
	from {{i}} 
        {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE {{to_epoch_milliseconds('current_timestamp()')}}  >= {{max_loaded}}
        {% endif %}  
    ) where row_num = 1 
{% if not loop.last %} union all {% endif %}
{% endfor %}