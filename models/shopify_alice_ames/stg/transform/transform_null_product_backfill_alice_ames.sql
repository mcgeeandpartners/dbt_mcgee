--This is the list of product titles that do not have product ids. We will use this model to union to the product tables.
with null_products as (

    select 
        'deleted' as product_status,
        product_title,
        order_line_item_id
    from {{ ref('base_null_product_backfill_alice_ames') }}
    where product_id is null

)

select 
      * 
    , (row_number() over (order by order_line_item_id) || '-' || product_title)::varchar as product_id
from null_products

