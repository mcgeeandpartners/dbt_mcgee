select distinct c.ID as CUSTOMER_ID,
c.FIRST_NAME as FIRST_NAME,
c.CREATED_AT as CREATED_AT,
c.EMAIL as EMAIL,
c.LAST_NAME as LAST_NAME,
c.STATE as IS_ACTIVE,
c.TAX_EXEMPT as IS_TAX_EXEMPT,
c.UPDATED_AT as UPDATED_AT,
ca.PHONE as PHONE,
ca.ADDRESS_1 as ADDRESS_1,
ca.ADDRESS_2 as ADDRESS_2,
ca.CITY as CITY,
ca.COUNTRY as COUNTRY,
ca.PROVINCE as STATE,
ca.ZIP as ZIPCODE,
ca.LATITUDE as LATITUDE,
ca.LONGITUDE as LONGITUDE,
'SHOPIFY' as CHANNEL_SOURCE
from
"AESTUARY_RAW"."SHOPIFY"."CUSTOMER" c inner join "AESTUARY_RAW"."SHOPIFY"."CUSTOMER_ADDRESS" ca on c.ID = ca.CUSTOMER_ID