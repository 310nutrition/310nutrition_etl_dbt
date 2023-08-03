-- Dependencies generated through pipeline script.
-- depends_on: {{ ref('dim_campaign_klaviyo') }}
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
    select 
    {{ dbt_utils.surrogate_key(['campaign_id','campaign_type']) }} AS campaign_key, 
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
    last_updated_date as effective_start_date,
    cast(null as date) as effective_end_date,
    current_timestamp() as last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
	from {{i}} 
        {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE {{to_epoch_milliseconds('current_timestamp()')}}  >= {{max_loaded}}
        {% endif %}  
{% if not loop.last %} union all {% endif %}
{% endfor %}