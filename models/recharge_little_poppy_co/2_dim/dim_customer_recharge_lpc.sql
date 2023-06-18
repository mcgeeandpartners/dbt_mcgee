{{config( alias='dim_customer' )}}

select
    {{ dbt_utils.generate_surrogate_key(['a.customer_id', 'b.address_id']) }} as customer_key,
    a.customer_id,
    b.address_id,
    a.customer_hash, 
    a.shopify_customer_id,
    b.payment_method_id,
    a.customer_email,
    nvl(a.customer_full_name, b.customer_full_name) as customer_full_name,
    a.subscriptions_active_count,
    a.subscriptions_total_count,
    a.has_valid_payment_method,
    a.has_payment_method_in_dunning,
    a.is_tax_exempt,
    b.address_line_1,
    b.address_line_2,
    b.city,
    b.province,
    b.zip,        
    b.country_code,
    b.company,
    b.phone,
    a.customer_created_at_utc,
    a.customer_updated_at_utc,
    b.address_created_at_utc,
    b.address_updated_at_utc,
    a.first_charge_processed_at_utc
from {{ ref('stg_customer_recharge_lpc') }} as a
left join {{ ref('stg_address_recharge_lpc') }} as b
    on a.customer_id = b.customer_id
    and b.is_recent = 1