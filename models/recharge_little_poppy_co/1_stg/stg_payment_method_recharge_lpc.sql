select 
    p.id as payment_method_id,
    p.customer_id,
    p.payment_type, 
    p.processor_customer_token, 
    p.processor_name,
    p.processor_payment_method_token,
    p.status as payment_method_status, 
    p.status_reason,
    p.billing_address_first_name,
    p.billing_address_last_name,
    p.billing_address_address_1,
    p.billing_address_address_2,
    p.billing_address_city,
    p.billing_address_province,
    p.billing_address_country,
    p.billing_address_zip,
    p.billing_address_company,
    p.billing_address_phone,
    p.billing_address_country_code,
    p.default::boolean as is_default_payment_method,
    p.payment_details_exp_year,
    p.payment_details_last_4,
    p.payment_details_exp_month,
    p.payment_details_brand,
    p.updated_at,
    p.created_at,
    p._fivetran_deleted,
    p._fivetran_synced
from {{ source('recharge_lpc', 'payment_method') }} as p
where not _fivetran_deleted