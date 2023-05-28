--For left over variant id that is null but product id is not null

with base as (
select 
    product_id,
    order_line_item_id,
    product_variant_name
from {{ ref('int_order_line_item_lpc') }}
where product_variant_id is null
qualify row_number() over (partition by product_variant_name order by order_line_item_id) = 1
)

select *
    , (row_number() over (order by order_line_item_id, product_variant_name) || '-' || product_variant_name)::varchar as product_variant_id
from base