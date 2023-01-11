select
    code,
    order_id,
    target_type,
    target_selection,
    type,
    allocation_method,
    value_type,
    title,
    value,
    description,
    index
from {{source('shopify_tenkara_usa', 'discount_application')}}