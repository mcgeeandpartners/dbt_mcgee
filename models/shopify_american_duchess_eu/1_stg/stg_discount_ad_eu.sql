select 
    id as discount_id,
    price_rule_id, 
    lower(code) as discount_code, 
    usage_count, 
    created_at as created_at_utc, 
    updated_at as updated_at_utc
    
from {{ source("shopify_ad_eu", "discount_code") }}
