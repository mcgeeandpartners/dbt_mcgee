select
    target_type,
    code,
    target_selection,
    type,
    allocation_method,
    value_type,
    title,
    order_id,
    value,
    description,
    index
from {{ source("SHOPIFY_AMERICAN_DUCHESS", "DISCOUNT_APPLICATION") }}