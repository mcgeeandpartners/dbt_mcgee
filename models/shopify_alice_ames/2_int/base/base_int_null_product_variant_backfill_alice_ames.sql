with null_product_ids as (
    select product_title, order_line_item_id, product_variant_id, product_variant_name
    from {{ ref('stg_order_line_item_alice_ames') }} 
    where product_id is null
)

--from the product ids that are null in oli table, we have 2 scenarios -> (a) product variant id is null -->244,633 (b) product variant id is not null --> 177,594
--we deal with each case differently

-- (a) product variant id is null -->244,203

, null_product_and_variant_ids as (
    select product_title, order_line_item_id, product_variant_name
    from null_product_ids
    where product_variant_id is null
    qualify row_number() over (partition by product_title, product_variant_name order by order_line_item_id) = 1
) 

, null_product_and_variant_ids_backfill as (
--this cte will be on the variant id level. There will be more than one product ids in the dataset
select 
	  order_line_item_id
    , product_title
    , (dense_rank() over (order by product_title) || '-' || product_title)::varchar as product_id
    , product_variant_name
	, (row_number() over (order by order_line_item_id, product_variant_name) || '-' || product_variant_name)::varchar as product_variant_id
    , true as is_null_product_variant_id --this is to differentiate when joining to order line and product tables
from null_product_and_variant_ids
) 


-- (b) product variant id is not null --> 177,594

, null_product_and_not_null_variant_ids as (
    select product_title, order_line_item_id, product_variant_id, product_variant_name
    from null_product_ids
    where product_variant_id is not null
    qualify row_number() over (partition by product_title, product_variant_name, product_variant_id order by order_line_item_id) = 1
) 

, null_product_and_not_null_variant_ids_backfill as (
select 
	  order_line_item_id
    , product_title
    , (dense_rank() over (order by product_title) || '-' || product_title)::varchar as product_id
    , product_variant_name
	, product_variant_id::varchar as product_variant_id
    , false as is_null_product_variant_id
from null_product_and_not_null_variant_ids
)

, unioned as (
    select * from null_product_and_variant_ids_backfill
    union
    select * from null_product_and_not_null_variant_ids_backfill
)

select *
from unioned 