{% if var('FBAReturnsReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
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
    {{set_table_name('%fbareturnsreport')}}    
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

        SELECT *, ROW_NUMBER() OVER (PARTITION BY date(return_date), asin, sku, order_id, fnsku, license_plate_number, fulfillment_center_id order by _daton_batch_runtime desc) as _seq_id 
        from (
            select * {{exclude()}} (row_num)
            from (
                select 
                '{{brand}}' as brand,
                '{{store}}' as store,
                CAST(ReportstartDate as timestamp) ReportstartDate,
                CAST(ReportendDate as timestamp) ReportendDate,
                CAST(ReportRequestTime as timestamp) ReportRequestTime,
                sellingPartnerId,
                marketplaceName,
                marketplaceId,
                cast(return_date as DATE) as return_date,
                coalesce(order_id,'') as order_id,
                coalesce(sku,'') as sku,
                coalesce(asin,'') as asin,
                coalesce(fnsku,'') as fnsku,
                product_name,
                quantity,
                coalesce(fulfillment_center_id,'') as fulfillment_center_id,
                detailed_disposition,
                reason,
                status,
                coalesce(license_plate_number,'') as license_plate_number,
                customer_comments,
	            {{daton_user_id()}} as _daton_user_id,
                {{daton_batch_runtime()}} as _daton_batch_runtime,
                {{daton_batch_id()}} as _daton_batch_id,
                current_timestamp() as _last_updated,
                '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
                Dense_Rank() OVER (PARTITION BY date(return_date), asin, sku, order_id, fnsku, license_plate_number, fulfillment_center_id, marketplaceId order by {{daton_batch_runtime()}} desc) row_num
                from {{i}}    
                    {% if is_incremental() %}
                    {# /* -- this filter will only be applied on an incremental run */ #}
                    WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
                    {% endif %}
                ) 
            where row_num = 1
        )
        {% if not loop.last %} union all {% endif %}
    {% endfor %}