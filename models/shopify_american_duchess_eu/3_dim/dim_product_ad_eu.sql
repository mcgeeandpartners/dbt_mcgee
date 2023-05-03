{{ config(alias="dim_product") }}

select
    pv.product_id,
    pv.product_variant_id,
    {{ dbt_utils.surrogate_key(['pv.product_variant_id', 'pv.product_id']) }} as product_key,
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
    pv.updated_at_utc as product_updated_at,
    {# these fields need to be updated when we have the SKU mapping document #}
    NULL as product_category,
    NULL as product_sub_type,
    NULL as product_heel,
    NULL as product_era,
    NULL as product_genre,
    NULL as product_decade,
    NULL as product_is_pre_order,
    NULL as product_color,
    NULL as product_made_in,
    NULL as product_manufacturer,
    i.inventory_item_id, 
    i.inventory_cost,
    i.inventory_available_quantity


from {{ ref('int_product_ad_eu') }} as p
left join {{ ref('int_product_variant_ad_eu') }} as pv 
    on p.product_id = pv.product_id
left join {{ ref('stg_inventory_item_level_ad_eu')}}  as i
    on nullif(lower(pv.product_variant_sku), '') = i.product_sku
    and i.is_most_recent = 1
