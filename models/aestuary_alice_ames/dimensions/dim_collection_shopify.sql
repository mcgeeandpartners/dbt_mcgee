with no_collection_cte as (

    select 
    md5(cast(coalesce(cast('COLLECTION_NOT_FOUND' as TEXT), '') || '-' || coalesce(cast(-1 as TEXT), '') as TEXT)) as COLLECTION_KEY,
    -1 as COLLECTION_ID,
    NULL as COLLECTION_NAME,
    NULL as COLLECTION_HANDLE,
    NULL as COLLECTION_UPDATED_DATE,
    NULL as COLLECTION_PUBLISHED_DATE
)

select
{{ dbt_utils.surrogate_key(['c.ID']) }} as COLLECTION_KEY,
c.ID as COLLECTION_ID,
c.TITLE as COLLECTION_NAME,
c.HANDLE as COLLECTION_HANDLE,
c.UPDATED_AT as COLLECTION_UPDATED_DATE,
c.PUBLISHED_AT as COLLECTION_PUBLISHED_DATE
from {{ source('ALICE_AMES_SHOPIFY', 'COLLECTION') }} c
union all
select * from no_collection_cte