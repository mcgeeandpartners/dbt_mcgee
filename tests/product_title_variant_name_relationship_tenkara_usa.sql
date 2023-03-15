select 
    product_variant_name, 
    count(distinct(product_title)) as c 
from {{ ref('stg_order_line_item_tenkara_usa') }}
group by 1 having c > 1