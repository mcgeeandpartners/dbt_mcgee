{{ config(alias="dim_product") }}

select
    pv.product_id,
    pv.product_variant_id,
    {{ dbt_utils.surrogate_key(['pv.product_variant_id', 'pv.product_id']) }} as product_key,
    p.product_title,
    p.product_handle,
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
    v.volume_proxy, 
    v.product_title_adj, 
    v.product_category, 
    v.product_type,
    v.product_sub_type, 
    v.product_heel, 
    v.product_era, 
    v.product_genre, 
    v.product_decade, 
    v.product_color, 
    v.product_is_pre_order, 
    v.product_style, 
    NULL as product_made_in,
    NULL as product_manufacturer,
    i.inventory_item_id, 
    i.inventory_cost,
    i.inventory_available_quantity


from {{ ref('transform_product_ad') }} as p
left join {{ ref('transform_product_variant_ad') }} as pv 
    on p.product_id = pv.product_id
--Following is for master SKU list fields
left join {{ ref('stg_master_sku_list_ad') }} as v
    on pv.product_variant_name = v.product_variant_name
--Get inventory levels
left join {{ ref('stg_inventory_item_level_ad')}}  as i
    on nullif(lower(pv.product_variant_sku), '') = i.product_sku
    and i.is_most_recent = 1
