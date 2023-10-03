{% snapshot dim_product_snapshot %}

{{
    config(
      target_schema='edm_modelling_snapshot',
      unique_key='product_key',
      strategy='check',
      check_cols=['start_date','end_date'],
    )
}}


{% set table_name_query %}
{{set_table_name_modelling('dim_product_%')}}
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}
select * {{exclude()}} (row_num, _daton_batch_runtime) from (
select *, row_number() over(partition by product_id,sku,platform_name, start_date, end_date order by _daton_batch_runtime desc) row_num from (
{% for i in results_list %}
        select 
        {{ dbt_utils.surrogate_key(['product_id','sku','platform_name']) }} AS product_key,
        platform_name,
        product_name,
        product_id,
        sku,
        color,
        seller,
        size,
        product_category,
        --product_status,
        --buybox_landed_price,
        --buybox_listing_price,
        --buybox_seller_id,
        description, 
        category, 
        sub_category, 
        mrp, 
        cogs, 
        start_date, 
        end_date,
        _daton_batch_runtime
	      from {{i}}
        
    {% if not loop.last %} union all {% endif %}
        
{% endfor %}
)) where row_num = 1
{% endsnapshot %}