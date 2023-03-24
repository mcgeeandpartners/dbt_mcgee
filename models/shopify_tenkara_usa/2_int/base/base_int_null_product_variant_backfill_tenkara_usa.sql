with null_product_ids as (
    select product_title, order_line_item_id, product_variant_id, product_variant_name
    from {{ ref('stg_order_line_item_tenkara_usa') }} 
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

--from the product ids that are null in oli table, unlike Alice Ames, we have only 1 scenario -> 
-- (a) product variant id is null 
-- (b) product variant id is not null --THIS IS NOT THE CASE AS OF TODAY, BUT WE WILL MAINTAIN THE CODE AS A BACKUP


-- (a) product variant id is null

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

---------------==================------------------------------==================---------------

--AS MENTIONED BEFORE, THE FOLLOWING 2 CTES WILL YEILD NO RECORDS. BUT WE WILL MAINTAIN THE CODE JUST IN CASE.

-- (b) product variant id is not null

, null_product_and_not_null_variant_ids as (
    select product_title, order_line_item_id, product_variant_id, product_variant_name
    from null_product_ids
    where product_variant_id is not null
    qualify row_number() over (partition by product_title, product_variant_name, product_variant_id order by order_line_item_id) = 1
) 

, null_product_and_not_null_variant_ids_backfill as (
select 
	  a.order_line_item_id
    , a.product_title
    , b.product_id
    , a.product_variant_name
	, a.product_variant_id::varchar as product_variant_id
    , false as is_null_product_variant_id
from null_product_and_not_null_variant_ids as a
inner join null_product_ids_backfill as b 
    on a.product_title = b.product_title
)

---------------==================------------------------------==================---------------

, unioned as (
    select * from null_product_and_variant_ids_backfill
    union
    select * from null_product_and_not_null_variant_ids_backfill
)

select *
from unioned 
