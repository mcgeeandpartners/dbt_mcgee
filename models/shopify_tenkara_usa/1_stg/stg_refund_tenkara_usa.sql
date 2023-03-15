select
    id as refund_id,
    {{ dbt_utils.surrogate_key(['id']) }} as refund_key,
    order_id
    user_id,
    created_at,
    processed_at,
    note,
    restock,
    _fivetran_synced,
    total_duties_set
from {{ source('shopify_tenkara_usa', 'refund') }}