with no_discount_cte as (

    select 
    md5(cast(coalesce(cast('NO_DISCOUNT' as TEXT), '') || '-' || coalesce(cast(-1 as TEXT), '') as TEXT)) as DISCOUNT_KEY,
    'NO_DISCOUNT' as DISCOUNT_CODE,
    -1 as PRICE_RULE_ID,
    NULL as DISCOUNT_CREATED_DATE,
    NULL as DISCOUNT_START_DATE,
    NULL as DISCOUNT_END_DATE,
    NULL as DISCOUNT_TARGET_TYPE,
    NULL as DISCOUNT_ALLOCATION_METHOD,
    NULL as DISCOUNT_VALUE_TYPE,
    NULL as DISCOUNT_VALUE,
    'SHOPIFY' as CHANNEL_SOURCE
)

select 
{{ dbt_utils.surrogate_key(['dc.CODE','pr.ID']) }} as DISCOUNT_KEY,
NULLIF(TRIM(dc.CODE),'') as DISCOUNT_CODE,
pr.ID as PRICE_RULE_ID,
pr.CREATED_AT as DISCOUNT_CREATED_DATE,
pr.STARTS_AT as DISCOUNT_START_DATE,
pr.ENDS_AT as DISCOUNT_END_DATE,
NULLIF(TRIM(pr.TITLE),'') as DISCOUNT_TARGET_TYPE,
NULLIF(TRIM(pr.TARGET_SELECTION),'') as DISCOUNT_ALLOCATION_METHOD,
NULLIF(TRIM(pr.VALUE_TYPE),'') as DISCOUNT_VALUE_TYPE,
pr.VALUE as DISCOUNT_VALUE,
'SHOPIFY' as CHANNEL_SOURCE
from {{ source('ALICE_AMES_SHOPIFY', 'DISCOUNT_CODE') }} dc inner join {{ source('ALICE_AMES_SHOPIFY', 'PRICE_RULE') }} pr on dc.PRICE_RULE_ID = pr.ID
union all 
select * from no_discount_cte