--We have to resolve issues for null product ids in this table.
--For som product titles, we have null product ids in some orders, but populated product ids in others. With following 3 ctes, we are getting those product titles so we can add them back in final cte.

with source as (
    select * from {{ source('shopify_ad_eu', 'order_line') }}
)

select
    oli.id as order_line_item_id,
    oli.order_id,
    oli.variant_id::varchar as product_variant_id,
    {# coalesce(oli.variant_id::varchar, pvib.product_variant_id) as product_variant_id, #}
    oli.product_id::varchar as product_id,
    {# coalesce(oli.product_id::varchar, pib.product_id) as product_id, #}
    lower(oli.variant_title) as product_variant_title,
    lower(oli.name) as product_variant_name,
    lower(oli.title) as product_title,
    oli.index as order_line_index,
    lower(oli.vendor) as order_line_item_vendor,
    case 
        when nvl(order_line_item_vendor, '') != 'route' then false
        when nvl(order_line_item_vendor, '') = 'route' then true
        else NULL
    end as is_vendor_route,
    oli.price as order_line_item_price,
    oli.quantity as order_line_item_units,
    iff(is_vendor_route = false, oli.price, 0) as order_line_item_price_product,
    iff(is_vendor_route = true, oli.price, 0) as order_line_item_price_route,
    iff(is_vendor_route = false, oli.quantity, 0) as order_line_item_units_product, --sum this up on order in fact table
    iff(is_vendor_route = true, oli.quantity, 0) as order_line_item_units_route,
    oli.price_set,
    oli.pre_tax_price,
    oli.pre_tax_price_set,
    nullif(lower(oli.sku), '') as sku,
    oli.total_discount,
    oli.total_discount_set,
    oli.destination_location_address_2,
    oli.fulfillable_quantity,
    oli.fulfillment_status,
    oli.fulfillment_service,
    oli.origin_location_address_2,
    oli.origin_location_city,
    oli.requires_shipping as is_shipping_required,
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
    oli.taxable as is_taxable,
    oli.variant_inventory_management,
    oli.origin_location_zip,
    oli.destination_location_address_1,
    oli.destination_location_city,
    oli.destination_location_province_code,
    oli.gift_card as is_gift_card,
    oli.origin_location_province_code,
    oli.tax_code,
    oli._fivetran_synced
    
from source as oli