-- Dependencies generated through pipeline script.
-- depends_on: {{ ref('fact_traffic_amazonseller') }}


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
{{set_table_name_modelling('fact_traffic%')}}
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
        {{ dbt_utils.surrogate_key(['product_id','sku','platform_name']) }} AS product_key,
        {{ dbt_utils.surrogate_key(['brand']) }} AS brand_key,
        {{ dbt_utils.surrogate_key(['platform_name','store_name']) }} AS platform_key,
        {{ dbt_utils.surrogate_key(['source','medium','campaign','keyword','content']) }} AS utm_key,
        {{ dbt_utils.surrogate_key(['event_name','landing_page_path']) }} AS event_key,
        date,
        mobile_sessions,
        browser_sessions,
        tablet_sessions,
        sessions,
        mobile_pageviews as mobile_page_views,
        browser_pageviews as browser_page_views,
        tablet_pageviews as tablet_page_views,
        pageviews as page_views,
        glance_views,
        buybox_percentage as buy_box_percentage,
        quantity as sold_quantity,
        round((product_sales/exchange_currency_rate),2) as revenue,
        current_timestamp() as last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
	      from {{i}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{to_epoch_milliseconds('current_timestamp()')}}  >= {{max_loaded}}
            {% endif %}
    {% if not loop.last %} union all {% endif %}
{% endfor %}