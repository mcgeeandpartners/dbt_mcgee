select
    destination_location_address_2,
    fulfillable_quantity,
    fulfillment_status,
    origin_location_address_2,
    origin_location_city,
    price,
    requires_shipping,
    origin_location_id,
    destination_location_id,
    order_id,
    price_set,
    pre_tax_price,
    origin_location_address_1,
    origin_location_country_code,
    variant_id,
    quantity,
    destination_location_country_code,
    destination_location_name,
    destination_location_zip,
    properties,
    product_exists,
    variant_title,
    vendor,
    grams,
    id,
    {{ dbt_utils.surrogate_key(['id', 'order_id']) }} as ORDER_LINE_ITEM_KEY,
    origin_location_name,
    product_id,
    taxable,
    variant_inventory_management,
    origin_location_zip,
    destination_location_address_1,
    pre_tax_price_set,
    sku,
    destination_location_city,
    destination_location_province_code,
    fulfillment_service,
    index,
    name,
    title,
    total_discount,
    total_discount_set,
    _fivetran_synced,
    gift_card,
    origin_location_province_code,
    tax_code
from {{ source("SHOPIFY_AMERICAN_DUCHESS", "ORDER_LINE") }}
