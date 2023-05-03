select 
    {{ dbt_utils.surrogate_key(['order_id', 'code']) }} as order_discount_code_id,
    order_id,
    lower(code) as discount_code,
    index,
    type, 
    amount,
    _fivetran_synced
    
from {{ source("shopify_ad_eu", "order_discount_code") }}
