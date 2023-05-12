select
    id as refund_id,
    {{ dbt_utils.generate_surrogate_key(['id']) }} as refund_key,
    order_id,
    user_id,
    note,
    restock,
    total_duties_set,
    created_at as created_at_utc,
    processed_at as processed_at_utc,
    _fivetran_synced
    
from {{ source('shopify_ad', 'refund') }}