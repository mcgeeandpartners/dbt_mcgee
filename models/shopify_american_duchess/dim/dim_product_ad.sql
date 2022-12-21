{{ config(alias="dim_product") }}

select
    p.id as product_id,
    pv.id as product_variant_id,
    pv.product_variant_key as product_key,
    nullif(trim(p.title), '') as product_title,
    nullif(trim(p.handle), '') as product_handle,
    nullif(trim(product_type), '') as product_type,
    nullif(trim(status), '') as product_status,
    nullif(trim(pv.title), '') as product_variant_title,
    pv.price as product_price,
    nullif(trim(pv.sku), '') as product_sku,
    pv.position as product_variant_position,
    pv.created_at as product_created_date,
    pv.updated_at as product_updated_date,
    nullif(trim(pv.barcode), '') as product_barcode,
    pv.grams as product_grams,
    pv.weight as product_weight,
    nullif(trim(pv.weight_unit), '') as product_weight_unit
from {{ ref("stg_product_ad") }} p
left join {{ ref("stg_product_variant_ad") }} pv on p.id = pv.product_id
