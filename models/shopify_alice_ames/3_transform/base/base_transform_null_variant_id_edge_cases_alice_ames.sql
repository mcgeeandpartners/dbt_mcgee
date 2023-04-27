--this model is separate on purpose and is only to deal with one specific edge case in order line table for now
select 
    order_line_item_id,
    product_id,
    (row_number() over (order by order_line_item_id, product_variant_name) || '-' || product_variant_name)::varchar as product_variant_id,
    product_variant_name
from {{ ref('int_order_line_item_alice_ames') }}
where product_variant_name = 'the short sleeve ballet dress in french vintage floral'
