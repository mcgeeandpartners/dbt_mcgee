with stg_product as (

    select *
    from {{ ref('stg_product_tenkara_usa') }}

)

, backfill_null_product_id as (

    select distinct 
        product_id, 
        product_title,
        'deleted' as product_status
    from {{ ref('base_int_null_product_variant_backfill_tenkara_usa') }}

)

, null_product as (
    select 
        product_id,
        null as product_type,
        product_status,
        null as published_scope,
        null as template_suffix,
        null as vendor,
        null as product_handle,
        null as body_html,
        product_title,
        null as published_at_utc,
        null as created_at_utc,
        null as updated_at_utc

    from backfill_null_product_id
)

select * from stg_product
union all 
select * from null_product