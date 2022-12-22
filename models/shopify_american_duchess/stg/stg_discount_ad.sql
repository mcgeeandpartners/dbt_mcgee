select created_at, 
    updated_at, 
    usage_count, 
    price_rule_id, 
    code, 
    id
from {{ source("SHOPIFY_AMERICAN_DUCHESS", "DISCOUNT_CODE") }}