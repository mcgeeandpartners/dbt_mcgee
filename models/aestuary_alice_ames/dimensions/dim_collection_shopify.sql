select
{{ dbt_utils.surrogate_key(['c.ID']) }} as COLLECTION_KEY,
c.ID as COLLECTION_ID,
c.TITLE as COLLECTION_NAME,
c.HANDLE as COLLECTION_HANDLE,
c.UPDATED_AT as COLLECTION_UPDATED_DATE,
c.PUBLISHED_AT as COLLECTION_PUBLISHED_DATE
from {{ source('ALICE_AMES_SHOPIFY', 'COLLECTION') }} c