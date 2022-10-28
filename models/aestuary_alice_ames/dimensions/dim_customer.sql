with customer_address_cte as (

  select *, ROW_NUMBER () OVER (PARTITION BY ca.CUSTOMER_ID ORDER BY ca.ID desc) AS TO_SELECT  from {{ source('ALICE_AMES_SHOPIFY', 'CUSTOMER_ADDRESS') }} ca order by ID desc

),

no_customer_cte as (

    select 
    md5(cast(coalesce(cast('CUSTOMER_NOT_FOUND' as TEXT), '') || '-' || coalesce(cast(-1 as TEXT), '') as TEXT)) as CUSTOMER_KEY,
    -1 as CUSTOMER_ID,
    NULL as CUSTOMER_FIRST_NAME,
    NULL as CUSTOMER_CREATED_DATE,
    NULL as CUSTOMER_EMAIL,
    NULL as CUSTOMER_LAST_NAME,
    NULL as CUSTOMER_ACTIVITY_STATE,
    NULL as CUSTOMER_IS_TAX_EXEMPT,
    NULL as CUSTOMER_UPDATED_DATE,
    NULL as CUSTOMER_PHONE,
    NULL as CUSTOMER_ADDRESS_1,
    NULL as CUSTOMER_ADDRESS_2,
    NULL as CUSTOMER_CITY,
    NULL as CUSTOMER_COUNTRY,
    NULL as CUSTOMER_STATE,
    NULL as CUSTOMER_ZIPCODE,
    NULL as CUSTOMER_LATITUDE,
    NULL as CUSTOMER_LONGITUDE,
    'SHOPIFY' as CHANNEL_SOURCE
)

select
{{ dbt_utils.surrogate_key(['c.ID']) }} as CUSTOMER_KEY,
c.ID as CUSTOMER_ID,
NULLIF(c.FIRST_NAME,'') as CUSTOMER_FIRST_NAME,
c.CREATED_AT as CUSTOMER_CREATED_DATE,
NULLIF(c.EMAIL,'') as CUSTOMER_EMAIL,
NULLIF(c.LAST_NAME,'') as CUSTOMER_LAST_NAME,
lower(c.STATE) as CUSTOMER_ACTIVITY_STATE,
c.TAX_EXEMPT as CUSTOMER_IS_TAX_EXEMPT,
c.UPDATED_AT as CUSTOMER_UPDATED_DATE,
NULLIF(ca.PHONE,'') as CUSTOMER_PHONE,
NULLIF(ca.ADDRESS_1,'') as CUSTOMER_ADDRESS_1,
NULLIF(ca.ADDRESS_2,'') as CUSTOMER_ADDRESS_2,
NULLIF(ca.CITY,'') as CUSTOMER_CITY,
NULLIF(ca.COUNTRY,'') as CUSTOMER_COUNTRY,
NULLIF(ca.PROVINCE ,'') as CUSTOMER_STATE,
NULLIF(ca.ZIP,'') as CUSTOMER_ZIPCODE,
NULLIF(ca.LATITUDE ,'') as CUSTOMER_LATITUDE,
NULLIF(ca.LONGITUDE ,'') as CUSTOMER_LONGITUDE,
'SHOPIFY' as CHANNEL_SOURCE
from
{{ source('ALICE_AMES_SHOPIFY', 'CUSTOMER') }} c left join customer_address_cte ca on c.ID = ca.CUSTOMER_ID and ca.TO_SELECT = 1

union all
select * from no_customer_cte