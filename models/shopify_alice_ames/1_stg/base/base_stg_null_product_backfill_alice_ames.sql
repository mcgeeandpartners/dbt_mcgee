--We have to resolve issues for null product ids in this table.
--For som product titles, we have null product ids in some orders, but populated product ids in others. With following 3 ctes, we are getting those product titles so we can add them back in final cte.

with source as (
    select * from {{ source('shopify_alice_ames', 'order_line') }}
),

null_products as (
    select 
        lower(trim(title)) as product_title, id as order_line_item_id
    from source as oli
    where product_id is null
    qualify row_number() over (partition by product_title order by order_line_item_id) = 1
),

not_null_products as (
    select distinct
        lower(trim(title)) as product_title, product_id
    from source as oli
    where product_id is not null
),

null_product_id_backfill as (
    select 
        a.*, b.product_id::varchar as product_id
    from null_products as a 
    left join not_null_products as b on a.product_title = b.product_title
)

select * 
from null_product_id_backfill