select 
    product_variant_name, 
    count(distinct(product_title)) as c 
from {{ ref('stg_order_line_item_ad') }}
group by 1 having c > 1