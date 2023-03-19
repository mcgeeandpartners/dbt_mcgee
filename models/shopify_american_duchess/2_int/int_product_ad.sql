with stg_product as (

    select *
    from {{ ref('stg_product_ad') }}

)

, backfill_null_product_id as (

    select distinct 
        product_id, 
        product_title,
        'deleted' as product_status
    from {{ ref('base_int_null_product_variant_backfill_ad') }}

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

-- These are product id edge cases that exist in order line table but are missing from products table. We will union these to products table.
, missing_not_null_product_ids as (
    select distinct
        a.product_id,
        null as product_type,
        'deleted' as product_status,
        null as published_scope,
        null as template_suffix,
        null as vendor,
        null as product_handle,
        null as body_html,
        a.product_title,
        null as published_at_utc,
        null as created_at_utc,
        null as updated_at_utc

from {{ ref('stg_order_line_item_ad') }} as a 
left join {{ ref('stg_product_ad') }} as b 
    on a.product_id = b.product_id
where a.product_id is not null and b.product_id is null

)

select * from stg_product
union all 
select * from null_product
union all 
select * from missing_not_null_product_ids
