--Do we have null variant id which has variant name matching another order where the id is not null?

with source as (
    select * from {{ source('shopify_tenkara_usa', 'order_line') }}
),

null_product_variants as (
    select 
        lower(trim(name)) as product_variant_name, id as order_line_item_id
    from source as oli
    where variant_id is null
    qualify row_number() over (partition by product_variant_name order by order_line_item_id) = 1
),

not_null_products_variants as (
    select distinct
        lower(trim(name)) as product_variant_name, variant_id as product_variant_id, id as order_line_item_id
    from source as oli
    where variant_id is not null
    qualify row_number() over (partition by product_variant_name order by order_line_item_id) = 1
),

null_product_variant_id_backfill as (
    select 
        a.*, b.product_variant_id::varchar as product_variant_id
    from null_product_variants as a 
    left join not_null_products_variants as b on a.product_variant_name = b.product_variant_name
)

select *
from null_product_variant_id_backfill