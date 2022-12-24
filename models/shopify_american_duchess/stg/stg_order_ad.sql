select
    id as order_id,
    {{ dbt_utils.surrogate_key(['id']) }} as order_key,
    created_at as order_placed_utc,
    cancel_reason,
    cancelled_at as cancelled_at_utc,
    fulfillment_status,
    customer_id,
    email,
    shipping_address_city,
    shipping_address_province,
    shipping_address_province_code,
    shipping_address_country,
    shipping_address_country_code,
    shipping_address_zip,
    billing_address_city,
    billing_address_country,
    billing_address_country_code,
    billing_address_province,
    billing_address_province_code,
    billing_address_zip,
    financial_status,
    currency,
    taxes_included,
    total_tax,
    total_price,
    total_line_items_price,
    total_discounts as discounts_total,
    current_total_price,
    total_price - current_total_price as refund_total,
    total_price + total_discounts - total_tax - total_line_items_price  as revenue_shipping
from {{ source("SHOPIFY_AMERICAN_DUCHESS", "ORDER") }} o
where not _fivetran_deleted
