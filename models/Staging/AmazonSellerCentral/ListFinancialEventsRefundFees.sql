{% if var('ListFinancialEventsRefundFees') %}
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

    with unnested_refundeventlist as (
    {% set table_name_query %}
    {{set_table_name('%listfinancialevents')}}    
    {% endset %}  

    {% set results = run_query(table_name_query) %}
    {% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0].values() %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}


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


        SELECT * FROM (
        select 
        a.* {{exclude()}} (_daton_user_id, _daton_batch_runtime, _daton_batch_id),
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then a.CurrencyCode else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            a.CurrencyCode as exchange_currency_code, 
        {% endif %}
        a._daton_user_id,
        a._daton_batch_runtime,
        a._daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from(
        select
        '{{brand}}' as brand,
        '{{store}}' as store,
        {% if target.type=='snowflake' %} 
            REFUNDEVENTLIST.VALUE:PostedDate :: DATE as posteddate,
            REFUNDEVENTLIST.VALUE:AmazonOrderId :: varchar as AmazonOrderId,
            REFUNDEVENTLIST.VALUE:MarketplaceName :: varchar as marketplacename,
            ShipmentItemAdjustmentList.VALUE:SellerSKU :: varchar as SellerSKU,
            ShipmentItemAdjustmentList.VALUE:QuantityShipped :: FLOAT as QuantityShipped,
            ItemFeeAdjustmentList.value:FeeType :: varchar as FeeType ,
            FeeAmount.value:CurrencyCode::varchar as CurrencyCode,
            FeeAmount.value:CurrencyAmount::FLOAT as CurrencyAmount,
        {% else %}
            date(RefundEventlist.posteddate) as posteddate,
            coalesce(RefundEventlist.amazonorderid,'') as amazonorderid,
            coalesce(RefundEventlist.marketplacename,'') as marketplacename,
            ShipmentItemAdjustmentList.sellerSKU as sellerSKU,
            ShipmentItemAdjustmentList.quantityshipped as quantityshipped,
            coalesce(ItemFeeAdjustmentList.FeeType,'') as FeeType,
            FeeAmount.CurrencyCode as CurrencyCode,
            FeeAmount.CurrencyAmount as CurrencyAmount,
        {% endif %}
	   	{{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id
        FROM  {{i}} 
                {{unnesting("REFUNDEVENTLIST")}}
                {{multi_unnesting("REFUNDEVENTLIST","ShipmentItemAdjustmentList")}}
                {{multi_unnesting("ShipmentItemAdjustmentList","ItemFeeAdjustmentList")}}
                {{multi_unnesting("ItemFeeAdjustmentList","FeeAmount")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
        )a
        {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(posteddate) = c.date and a.CurrencyCode = c.to_currency_code
        {% endif %}
        )
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
    )

    select *, ROW_NUMBER() OVER (PARTITION BY posteddate, marketplacename, amazonorderid order by _daton_batch_runtime, FeeType, quantityshipped) as _seq_id 
    from (
        select * {{exclude()}} (rank) from (
            select *,
            DENSE_RANK() OVER (PARTITION BY posteddate, marketplacename, amazonorderid, FeeType order by _daton_batch_runtime desc) rank
            from unnested_refundeventlist
            ) where rank=1
    )
