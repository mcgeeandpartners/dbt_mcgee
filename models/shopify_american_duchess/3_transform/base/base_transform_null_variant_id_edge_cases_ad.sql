--These are all the null variant ids with not null product ids. These cases are not covered under the previous int and stg models
select 
    product_id,
    product_title,
    product_variant_name,
    order_line_item_id,
    (row_number() over (order by order_line_item_id, product_variant_name) || '-' || product_variant_name)::varchar as product_variant_id
from {{ ref('int_order_line_item_ad') }}
where 1 = 1
    and product_id is not null
    and product_variant_id is null
qualify row_number() over (partition by product_id, product_variant_name, product_title order by order_line_item_id) = 1