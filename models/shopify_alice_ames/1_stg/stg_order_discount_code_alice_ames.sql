select 
    {{ dbt_utils.generate_surrogate_key(['order_id', 'code']) }} as order_discount_code_id,
    order_id,
    lower(code) as discount_code,
    index,
    type, 
    amount,
    _fivetran_synced
    
from {{ source("shopify_alice_ames", "order_discount_code") }}