{% if var('FacebookAdinsightsAction') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
{{set_table_name('%facebookads%adinsights')}}    
{% endset %}  



{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
{% set results_list = [] %}
{% set tables_lowercase_list = [] %}
{% endif %}

with fbadinsights as (
{% for i in results_list %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours')%}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

    SELECT * 
    From (
        select
        '{{brand}}' as brand,
        '{{store}}' as store,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then account_currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            account_currency as exchange_currency_code, 
        {% endif %}
        a.* from (
        select 
        account_currency,
        account_id,
        account_name,
        action_values,
        {% if target.type =='snowflake' %}
        ACTIONS.VALUE:"1d_click"::VARCHAR as action_1d_click,
        ACTIONS.VALUE:"1d_view"::VARCHAR as action_1d_view,
        ACTIONS.VALUE:"7d_click"::VARCHAR as action_7d_click,
        ACTIONS.VALUE:"7d_view"::VARCHAR as action_7d_view,
        ACTIONS.VALUE:"28d_click"::VARCHAR as action_28d_click,
        ACTIONS.VALUE:"28d_view"::VARCHAR as action_28d_view,
        ACTIONS.VALUE:action_canvas_component_id::VARCHAR as action_action_canvas_component_id,
        ACTIONS.VALUE:action_canvas_component_name::VARCHAR as action_action_canvas_component_name,
        ACTIONS.VALUE:action_carousel_card_id::VARCHAR as action_action_carousel_card_id,
        ACTIONS.VALUE:action_carousel_card_name::VARCHAR as action_action_carousel_card_name,
        ACTIONS.VALUE:action_destination::VARCHAR as action_action_destination,
        ACTIONS.VALUE:action_device::VARCHAR as action_action_device,
        ACTIONS.VALUE:action_event_channel::VARCHAR as action_action_event_channel,
        ACTIONS.VALUE:action_link_click_destination::VARCHAR as action_action_link_click_destination,
        ACTIONS.VALUE:action_location_code::VARCHAR as action_action_location_code,
        ACTIONS.VALUE:action_reaction::VARCHAR as action_action_reaction,
        ACTIONS.VALUE:action_target_id::VARCHAR as action_action_target_id,
        coalesce(ACTIONS.VALUE:action_type::VARCHAR,'') as action_action_type,
        ACTIONS.VALUE:action_video_asset_id::VARCHAR as action_action_video_asset_id,
        ACTIONS.VALUE:action_video_sound::VARCHAR as action_action_video_sound,
        ACTIONS.VALUE:action_video_type::VARCHAR as action_action_video_type,
        ACTIONS.VALUE:inline::VARCHAR as action_inline,
        ACTIONS.VALUE:value::VARCHAR as action_value,
        ACTIONS.VALUE:id::VARCHAR as action_id,
        {% else %}
        actions._daton_pre_1d_click as action_1d_click,
        actions._daton_pre_1d_view as action_1d_view,
        actions._daton_pre_7d_click as action_7d_click,
        actions._daton_pre_7d_view as action_7d_view,
        actions._daton_pre_28d_click as action_28d_click,
        actions._daton_pre_28d_view as action_28d_view,
        actions.action_canvas_component_id as action_action_canvas_component_id,
        actions.action_canvas_component_name as action_action_canvas_component_name,
        actions.action_carousel_card_id as action_action_carousel_card_id,
        actions.action_carousel_card_name as action_action_carousel_card_name,
        actions.action_destination as action_action_destination,
        actions.action_device as action_action_device,
        actions.action_event_channel as action_action_event_channel,
        actions.action_link_click_destination as action_action_link_click_destination,
        actions.action_location_code as action_action_location_code,
        actions.action_reaction as action_action_reaction,
        actions.action_target_id as action_action_target_id,
        actions.action_type as action_action_type,
        actions.action_video_asset_id as action_action_video_asset_id,
        actions.action_video_sound as action_action_video_sound,
        actions.action_video_type as action_action_video_type,
        actions.inline as action_inline,
        actions.value as action_value,
        actions.id as action_id,
        {% endif %}
        coalesce(ad_id,'') as ad_id,
        ad_name,
        adset_id,
        adset_name,
        buying_type,
        campaign_id,
        campaign_name,
        canvas_avg_view_percent,
        clicks,
        cost_per_estimated_ad_recallers,
        cost_per_inline_link_click,
        cost_per_inline_post_engagement,
        cost_per_unique_click,
        cost_per_unique_inline_link_click,
        conversion_values,
        conversions,
        cpc,
        cpm,
        cpp,
        ctr,
        CAST(date_start as DATE) date_start,
        CAST(date_stop as DATE) date_stop,
        estimated_ad_recall_rate,
        estimated_ad_recallers,
        frequency,
        impressions,
        inline_link_clicks,
        inline_post_engagement,
        objective,
        reach,
        social_spend,
        spend,
        unique_clicks,
        unique_ctr,
        unique_inline_link_click_ctr,
        unique_inline_link_clicks,
        website_ctr,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        {% if target.type =='snowflake' %}
        ROW_NUMBER() OVER (PARTITION BY ad_id,date_start,actions.VALUE:action_type order by {{daton_batch_runtime()}} desc) row_num
        {% else %}
        ROW_NUMBER() OVER (PARTITION BY ad_id,date_start,actions.action_type order by {{daton_batch_runtime()}} desc) row_num
        {% endif %}
        from {{i}}
            {{unnesting("ACTIONS")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
    
        ) a
        {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(date_start) = c.date and account_currency = c.to_currency_code
        {% endif %}
    )
    {% if not loop.last %} union all {% endif %}
{% endfor %}
)

select * {{exclude()}}(row_num)
from fbadinsights 
where row_num =1

