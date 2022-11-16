with no_collection_product_cte as (

    select -1 as COLLECTION_ID,
    -1 as PRODUCT_ID
)

select COLLECTION_ID, PRODUCT_ID
from {{ source('ALICE_AMES_SHOPIFY', 'COLLECTION_PRODUCT') }}
union all
select * from no_collection_product_cte