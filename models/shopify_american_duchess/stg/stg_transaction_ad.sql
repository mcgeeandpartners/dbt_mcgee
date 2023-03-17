select 
    {{ dbt_utils.surrogate_key(['id']) }} as transaction_key,
    id as transaction_id,
    order_id,
    refund_id,
    user_id,
    parent_id as parent_transaction_id,
    amount,
    authorization,
    device_id,
    gateway,
    source_name,
    message,
    currency,
    location_id,
    payment_avs_result_code,
    payment_credit_card_bin,
    payment_cvv_result_code,
    payment_credit_card_number,
    payment_credit_card_company,
    kind,
    receipt,
    currency_exchange_id,
    currency_exchange_adjustment,
    currency_exchange_original_amount,
    currency_exchange_final_amount,
    currency_exchange_currency,
    error_code,
    status,
    test,
    created_at as created_at_utc,
    processed_at as processed_at_utc,    
    _fivetran_synced
from {{ source('shopify_ad', 'transaction') }}    