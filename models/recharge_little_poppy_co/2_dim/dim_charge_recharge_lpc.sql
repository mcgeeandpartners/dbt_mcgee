{{config( alias='dim_charges' )}}

select 
    {{ dbt_utils.generate_surrogate_key(['charge_id']) }} as charge_key,
    charge_id,
    customer_hash,
    shopify_order_id,
    external_transaction_id_payment_processor,
    charge_type,
    charge_status,
    note,
    payment_processor_name,
    has_uncommitted_changes,
    retry_date,
    error_type,
    client_details_browser_ip,
    client_details_user_agent,
    tags,
    error,
    external_variant_id_not_found,
    last_charge_attempt_date,
    charge_created_at_utc,
    charge_updated_at_utc,
    charge_scheduled_at_utc,
    charge_processed_at_utc

from {{ ref('stg_charge_recharge_lpc') }}