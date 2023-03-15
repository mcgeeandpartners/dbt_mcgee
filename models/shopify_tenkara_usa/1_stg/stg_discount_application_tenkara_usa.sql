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
from {{source('shopify_tenkara_usa', 'discount_application')}}
qualify row_number() over (partition by code, order_id, target_type, target_selection, type, allocation_method, value_type, title, value, description order by index desc) = 1