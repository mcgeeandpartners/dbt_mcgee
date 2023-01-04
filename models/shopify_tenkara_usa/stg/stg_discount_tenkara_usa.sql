select 
    id,
    price_rule_id, 
    code, 
    usage_count, 
    created_at, 
    updated_at
from {{ source("shopify_tenkara_usa", "discount_code") }}