
{% if var('recharge_flag') %}
-- depends_on: {{ ref('RechargeOrdersLineItemsProperties') }}
{% endif %}

{% if var('upscribe_flag') %}
-- depends_on: {{ ref('UpscribeSubscriptionItems') }}
{% endif %}

select * {{exclude()}} (row_num) from 
    (
    select
    'Shopify' as platform_name,
    ord.order_id,
    payment_gateway_names as payment_mode,
    {% if var('recharge_flag') %}
    recharge.order_channel,
    {% elif var('upscribe_flag') %}
    upscribe.order_channel,
    {% else %}
    'Online Store' as order_channel,
    {% endif %}
    date(updated_at) as last_updated_date,
    row_number() over(partition by ord.order_id order by ord._daton_batch_runtime desc) row_num
    from {{ ref('ShopifyOrders') }} ord

    {% if var('recharge_flag') %}
    left join (
    select distinct 'Recharge' as order_channel, 
    external_order_id as order_id
    from {{ ref('RechargeOrdersLineItemsProperties') }}) recharge
    on ord.order_id = recharge.order_id

    {% elif upscribe_flag %}
    left join (
    select distinct 'Upscribe' as order_channel, 
    cast(shopify_order_id as string) as order_id,
    from {{ ref('UpscribeSubscriptionItems') }}) upscribe 
    on ord.order_id = upscribe.order_id
    {% endif %}
    )
where row_num = 1


