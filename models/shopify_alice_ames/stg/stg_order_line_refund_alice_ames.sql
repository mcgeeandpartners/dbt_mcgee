select 
    id as order_line_refund_id,
    {{ dbt_utils.surrogate_key(['id', 'order_line_id', 'refund_id']) }} as order_line_refund_key,
    order_line_id as order_line_item_id,
    refund_id,
    location_id,
    quantity,
    restock_type,
    subtotal,
    total_tax,
    _fivetran_synced
from {{ source('shopify_alice_ames', 'order_line_refund') }}