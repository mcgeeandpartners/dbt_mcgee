{{ config(alias="dim_product") }}

{# with variant_name_id_mapping as (
    select m.product_variant_name, m.product_title, oli.product_variant_id, order_line_item_id
    from {{ ref('stg_master_sku_list_tenkara_usa') }} as m
    left join {{ ref('transform_order_line_item_tenkara_usa') }} as oli 
    	on m.product_variant_name = oli.product_variant_name
        and m.product_title = oli.product_title
    --a product variant and proudct title combo can belong to >1 distinct varinat ids. So we dedupe them using qualify. It doesnt really affect anything. We just need to dedepe them.         
    qualify row_number() over (partition by m.product_title, oli.product_variant_id order by oli.order_line_item_id) = 1
    ) #}

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
    pv.updated_at_utc as product_updated_at
    {# m.product_category,
    m.product_sub_type,
    m.product_campaign,
    m.campaign_year,
    m.is_print,
    m.print_solid_name,
    m.base_fabric_color,
    m.made_in,
    m.manufacturer  #}

from {{ ref('transform_product_tenkara_usa') }} as p
left join {{ ref('transform_product_variant_tenkara_usa') }} as pv 
    on p.product_id = pv.product_id
--Following is for master SKU list fields
{# left join variant_name_id_mapping as v 
    on pv.product_variant_id = v.product_variant_id
    and v.product_title = p.product_title
left join {{ ref('stg_master_sku_list_alice_ames') }} as m 
    on v.product_variant_name = m.product_variant_name #}