{% snapshot dim_subscriptions_snapshot %}

{{
    config(
      target_schema='edm_modelling_snapshot',
      unique_key='subscription_key',
      strategy='check',
      check_cols=['cancelled_at'],
    )
}}

{% set table_name_query %}
{{set_table_name_modelling('dim_subscription%')}}
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{% for i in results_list %}

    SELECT * {{exclude()}}(row_num) from (
    select 
    distinct
    {{ dbt_utils.surrogate_key(['subscription_id','sku']) }} AS subscription_key,
    brand,
    platform_name,
    store_name,
    order_channel,
    subscription_id,
    customer_id,
    utm_source,
    utm_medium,
    created_at,
    next_charge_scheduled_at,
    order_interval_frequency,
    external_product_id,
    sku,
    --order_day_of_month,
    order_interval_unit,
    status,
    updated_at,
    cancelled_at,
    cancellation_reason,
    cancellation_reason_comments,
    presentment_currency,
    row_number() over(partition by subscription_id,external_product_id,sku,platform_name order by _daton_batch_runtime desc) row_num
    from {{i}}) where row_num = 1
    
    {% if not loop.last %} union all {% endif %}
        
{% endfor %}

{% endsnapshot %}