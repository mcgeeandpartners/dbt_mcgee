{{
  config(
    materialized = 'table'
    )
}}

select 
    oli.order_line_item_id,
    oli.order_id,
    coalesce(oli.product_variant_id, b.product_variant_id) as product_variant_id,
    oli.product_variant_title,
    oli.product_variant_name,
    oli.product_id,
    oli.product_title,
    oli.order_line_index,
    oli.order_line_item_vendor,
    oli.is_vendor_route,
    oli.order_line_item_price,
    oli.order_line_item_units,
    oli.order_line_item_price_product,
    oli.order_line_item_price_route,
    oli.order_line_item_units_product, --sum this up on order in fact table
    oli.order_line_item_units_route,
    oli.price_set,
    oli.pre_tax_price,
    oli.pre_tax_price_set,
    oli.sku,
    oli.total_discount,
    oli.total_discount_set,
    oli.destination_location_address_2,
    oli.fulfillable_quantity,
    oli.fulfillment_status,
    oli.origin_location_address_2,
    oli.origin_location_city,
    oli.is_shipping_required,
    oli.origin_location_id,
    oli.destination_location_id,
    oli.origin_location_address_1,
    oli.origin_location_country_code,
    oli.destination_location_country_code,
    oli.destination_location_name,
    oli.destination_location_zip,
    oli.properties,
    oli.product_exists,
    oli.grams,
    oli.origin_location_name,
    oli.is_taxable,
    oli.variant_inventory_management,
    oli.origin_location_zip,
    oli.destination_location_address_1,
    oli.destination_location_city,
    oli.destination_location_province_code,
    oli.is_gift_card,
    oli.origin_location_province_code,
    oli.tax_code,
    oli._fivetran_synced

from {{ ref('int_order_line_item_alice_ames') }} as oli
left join {{ ref('base_transform_null_variant_id_edge_cases_alice_ames') }} as b on oli.order_line_item_id = b.order_line_item_id

