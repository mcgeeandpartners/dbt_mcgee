select
    id as order_adjustment_id,
    order_id,
    refund_id,
    amount,
    tax_amount,
    kind,
    reason,
    amount_set,
    tax_amount_set,
    _fivetran_synced
from {{ source('shopify_tenkara_usa', 'order_adjustment') }}