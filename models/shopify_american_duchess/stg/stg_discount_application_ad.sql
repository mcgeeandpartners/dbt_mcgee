--Order id is repeated in this table which is why we are deduping to take the most recent one from the duped rows
-- discount_code is null in this model. We will use 'order_discount_code' table to get the codes for missing discount codes

select
    lower(code) as discount_code,
    order_id,
    target_type,
    target_selection,
    type,
    allocation_method,
    value_type,
    lower(title) as title,
    value,
    lower(description) as description,
    index
from {{source('shopify_ad', 'discount_application')}}

qualify row_number() over (partition by code, order_id, target_type, target_selection, type, allocation_method, value_type, title, value, description order by index desc) = 1
