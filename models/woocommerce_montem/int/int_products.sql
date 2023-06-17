--  List of order lines with product_id = 0

with base as (
select name as product_title, id as order_line_item_id, variation_id as product_variant_id
    from  {{ref('stg_order_line_item_montem')}}
    where product_id = 0
),
product_ids_dedeuped as (
select product_title, order_line_item_id
    from base
    qualify row_number() over (partition by product_title order by order_line_item_id) = 1
),
product_id_cte as (
select 
          product_title
        , (row_number() over (order by order_line_item_id) || '-' || product_title)::varchar as product_id
    from product_ids_dedeuped
)
select b.order_line_item_id, b.product_title, pic.product_id
from base b
left join product_id_cte pic
on b.product_title = pic.product_title