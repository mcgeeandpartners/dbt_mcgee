with base as (
    select product_title
    from {{ ref('stg_master_sku_list_alice_ames') }}
    group by 1 having count(distinct(product_category || product_type || product_sub_type)) > 1
)

select * 
from base