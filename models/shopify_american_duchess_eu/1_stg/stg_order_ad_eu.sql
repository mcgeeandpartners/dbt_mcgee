{{
  config(
    materialized = 'table'
    )
}}

select
    id as order_id,
    customer_id,
    app_id,
    location_id,
    token as order_token,
    nullif(cancel_reason, '') as order_cancel_reason,
    nullif(fulfillment_status, '') as order_fulfillment_status,
    nullif(email, '') as email,
    order_number,
    nullif(shipping_address_address_1, '') as shipping_address_address_1,
    nullif(shipping_address_address_2, '') as shipping_address_address_2,
    nullif(shipping_address_city, '') as shipping_address_city,
    nullif(shipping_address_company, '') as shipping_address_company,
    nullif(shipping_address_province, '') as shipping_address_province,
    nullif(shipping_address_province_code, '') as shipping_address_province_code,
    nullif(shipping_address_name, '') as shipping_address_name,
    nullif(shipping_address_country, '') as shipping_address_country,
    nullif(shipping_address_country_code, '') as shipping_address_country_code,
    nullif(shipping_address_zip, '') as shipping_address_zip,
    nullif(billing_address_address_1, '') as billing_address_address_1,
    nullif(billing_address_address_2, '') as billing_address_address_2,
    nullif(billing_address_city, '') as billing_address_city,
    nullif(billing_address_company, '') as billing_address_company,
    nullif(billing_address_province, '') as billing_address_province,
    nullif(billing_address_province_code, '') as billing_address_province_code,
    nullif(billing_address_name, '') as billing_address_name,
    nullif(billing_address_country, '') as billing_address_country,
    nullif(billing_address_country_code, '') as billing_address_country_code,
    nullif(billing_address_zip, '') as billing_address_zip,
    nullif(billing_address_phone, '') as billing_address_phone,
    source_name,
    browser_ip,
    nullif(financial_status, '') as order_financial_status,
    o.currency,
    o.presentment_currency, 
    nullif(referring_site, '') as referring_site,
    landing_site_base_url,
    note,
    processing_method,
    taxes_included,

    --Besides the usd and presentment fields, all other are currency fields in euros
    o.total_line_items_price as total_line_items_price_eur,
    o.total_discounts as total_discounts_eur,
    o.subtotal_price as subtotal_price_eur,
    o.total_price as total_price_eur,
    o.total_tax as total_tax_eur,
    o.current_total_price as current_total_price_eur,
    o.current_subtotal_price as current_subtotal_price_eur,
    o.current_total_discounts as current_total_discounts_eur,
    o.current_total_tax as current_total_tax_eur,
    total_price_eur - current_total_price_eur as refund_total_eur,
    total_price_eur + total_discounts_eur - total_tax_eur - total_line_items_price_eur  as revenue_shipping_eur,

    --converting the euro fields in usd based on the implied rate
    total_line_items_price_eur * b.implied_eur_to_usd_rate_per_order as total_line_items_price,
    total_discounts_eur * b.implied_eur_to_usd_rate_per_order as total_discounts,
    subtotal_price_eur * b.implied_eur_to_usd_rate_per_order as subtotal_price,
    total_price_eur * b.implied_eur_to_usd_rate_per_order as total_price,
    o.total_price_usd,
    total_price_set:presentment_money:amount::varchar as total_presentment_price,
    total_tax_eur * b.implied_eur_to_usd_rate_per_order as total_tax,
    current_total_price_eur * b.implied_eur_to_usd_rate_per_order as current_total_price,
    current_subtotal_price_eur * b.implied_eur_to_usd_rate_per_order as current_subtotal_price,
    current_total_discounts_eur * b.implied_eur_to_usd_rate_per_order as current_total_discounts,
    current_total_tax_eur * b.implied_eur_to_usd_rate_per_order as current_total_tax,
    refund_total_eur  * b.implied_eur_to_usd_rate_per_order as refund_total,
    revenue_shipping_eur  * b.implied_eur_to_usd_rate_per_order as revenue_shipping,

    total_shipping_price_set,
    created_at as order_placed_at_utc,
    cancelled_at as cancelled_at_utc,
    closed_at as closed_at_utc,
    processed_at as processed_at_utc,
    updated_at as updated_at_utc
  
from {{ source('shopify_ad_eu', 'order') }} as o
left join {{ ref('base_stg_usd_conversion_rate_per_order_eu') }} as b 
    on o.id = b.order_id
where not _fivetran_deleted
