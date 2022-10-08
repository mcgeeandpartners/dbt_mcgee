with customer_address_cte as (

  select *, ROW_NUMBER () OVER (PARTITION BY ca.CUSTOMER_ID ORDER BY ca.ID desc) AS TO_SELECT  from "AESTUARY_RAW"."SHOPIFY"."CUSTOMER_ADDRESS" ca order by ID desc

)
select
{{ dbt_utils.surrogate_key(['c.ID']) }} as CUSTOMER_KEY,
c.ID as CUSTOMER_ID,
c.FIRST_NAME as CUSTOMER_FIRST_NAME,
c.CREATED_AT as CUSTOMER_CREATED_AT,
c.EMAIL as CUSTOMER_EMAIL,
c.LAST_NAME as CUSTOMER_LAST_NAME,
lower(c.STATE) as CUSTOMER_ACTIVITY_STATE,
c.TAX_EXEMPT as CUSTOMER_IS_TAX_EXEMPT,
c.UPDATED_AT as CUSTOMER_UPDATED_AT,
case when ca.PHONE = '' then NULL else ca.PHONE end as CUSTOMER_PHONE,
case when ca.ADDRESS_1 = '' then NULL else ca.ADDRESS_1 end as CUSTOMER_ADDRESS_1,
case when ca.ADDRESS_2 = '' then NULL else ca.ADDRESS_2 end as CUSTOMER_ADDRESS_2,
case when ca.CITY = '' then NULL else ca.CITY end as CUSTOMER_CITY,
case when ca.COUNTRY = '' then NULL else ca.COUNTRY end as CUSTOMER_COUNTRY,
case when ca.PROVINCE = '' then NULL else ca.PROVINCE end as CUSTOMER_STATE,
case when ca.ZIP = '' then NULL else ca.ZIP end as CUSTOMER_ZIPCODE,
case when ca.LATITUDE = '' then NULL else ca.LATITUDE end as CUSTOMER_LATITUDE,
case when ca.LONGITUDE = '' then NULL else ca.LONGITUDE end as CUSTOMER_LONGITUDE,
'SHOPIFY' as CHANNEL_SOURCE
from
"AESTUARY_RAW"."SHOPIFY"."CUSTOMER" c left join customer_address_cte ca on c.ID = ca.CUSTOMER_ID and ca.TO_SELECT = 1