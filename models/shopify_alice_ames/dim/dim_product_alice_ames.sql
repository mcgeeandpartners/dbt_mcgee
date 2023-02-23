{{ config(alias="dim_product") }}

select
    p.product_id,
    pv.product_variant_id,
    {{ dbt_utils.surrogate_key(['pv.product_variant_id', 'p.product_id']) }} as product_key,
    p.product_title,
    p.product_handle,
    p.product_type,
    p.product_status,
    pv.product_variant_title,
    pv.price as product_price,
    nullif(lower(pv.product_variant_sku), '') as product_sku,
    pv.product_variant_position as product_position,
    pv.barcode as product_barcode,
    pv.grams as product_grams,
    pv.weight as product_weight,
    pv.weight_unit as product_weight_unit,
    pv.is_taxable,
    pv.is_shipping_required,
    pv.option_1 as product_option_1,
    pv.option_2 as product_option_2,
    pv.option_3 as product_option_3,
    pv.compare_at_price,
    pv.created_at_utc as product_created_at,
    pv.updated_at_utc as product_updated_at

from {{ ref('stg_product_alice_ames') }} as p
left join {{ ref('stg_product_variant_alice_ames') }} as pv 
    on p.product_id = pv.product_id