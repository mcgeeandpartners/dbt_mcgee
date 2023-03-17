select
    id as customer_id,
    first_name,
    last_name,
    lower(verified_email) as verified_email,
    lower(email) as email,
    phone,
    lower(state) as customer_activity_state,
    currency,
    total_spent,
    lifetime_duration,
    orders_count,
    tax_exempt,
    marketing_opt_in_level,
    multipass_identifier,
    can_delete,
    metafield,
    accepts_marketing,
    accepts_marketing_updated_at as accepts_marketing_updated_at_utc,
    email_marketing_consent_state,
    email_marketing_consent_consent_updated_at as email_marketing_consent_consent_updated_at_utc,
    email_marketing_consent_opt_in_level,
    note,
    updated_at as updated_at_utc,
    created_at as created_at_utc,
    _fivetran_synced
    
from {{source('shopify_ad', 'customer')}}
where not _fivetran_deleted