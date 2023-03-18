{{ config(alias="dim_product") }}

with variant_name_id_mapping as (
    select m.*, oli.order_line_item_id
    from {{ ref('stg_master_sku_list_alice_ames') }} as m
    left join {{ ref('transform_order_line_item_alice_ames') }} as oli 
    	on m.product_variant_name = oli.product_variant_name
        and m.product_title = oli.product_title
        and m.product_variant_id = oli.product_variant_id
        and m.product_id = oli.product_id
    --a product variant, product id and proudct title combo can belong to >1 distinct varinat names in 5 cases approx. So we dedupe them using qualify. It doesnt really affect anything. We just need to dedepe them.         
    qualify row_number() over (partition by m.product_title, m.product_variant_id, m.product_id order by oli.order_line_item_id) = 1
    )

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
    v.product_category,
    v.product_sub_type,
    v.product_campaign,
    v.campaign_year,
    v.is_print,
    v.print_solid_name,
    v.base_fabric_color,
    v.made_in,
    v.manufacturer 

from {{ ref('transform_product_alice_ames') }} as p
inner join {{ ref('transform_product_variant_alice_ames') }} as pv 
    on p.product_id = pv.product_id
--Following is for master SKU list fields
left join variant_name_id_mapping as v 
    on pv.product_variant_id = v.product_variant_id
    and pv.product_id = v.product_id
    and p.product_title = v.product_title
