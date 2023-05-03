with null_product_ids as (
    select product_title, order_line_item_id, product_variant_id, product_variant_name
    from {{ ref('stg_order_line_item_ad_eu') }} 
    where product_id is null
)

--generate the product ids based on title and ordered by the line item
, null_product_ids_deduped as (
    select product_title, order_line_item_id
    from null_product_ids
    qualify row_number() over (partition by product_title order by order_line_item_id) = 1
),

null_product_ids_backfill as (
    select 
          product_title
        , (row_number() over (order by order_line_item_id) || '-' || product_title)::varchar as product_id
    from null_product_ids_deduped
)

--from the product ids that are null in oli table, we have 2 scenarios -> (a) product variant id is null -->244,633 (b) product variant id is not null --> 177,594
--we deal with each case differently

-- product variant id is null

, null_product_and_variant_ids as (
    select product_title, order_line_item_id, product_variant_name
    from null_product_ids
    where product_variant_id is null
    qualify row_number() over (partition by product_title, product_variant_name order by order_line_item_id) = 1
) 

, null_product_and_variant_ids_backfill as (
--this cte will be on the variant id level. There will be more than one product ids in the dataset
select 
	  a.order_line_item_id
    , a.product_title
    , b.product_id
    , a.product_variant_name
	, (row_number() over (order by a.order_line_item_id, a.product_variant_name) || '-' || a.product_variant_name)::varchar as product_variant_id
    , true as is_null_product_variant_id --this is to differentiate when joining to order line and product tables
from null_product_and_variant_ids as a 
inner join null_product_ids_backfill as b 
    on a.product_title = b.product_title
) 

select *
from null_product_and_variant_ids_backfill