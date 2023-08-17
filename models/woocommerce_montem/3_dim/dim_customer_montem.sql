{{ config(alias="dim_customer") }}

SELECT ID as customer_id,
    {{ dbt_utils.generate_surrogate_key(['ID']) }} as customer_key,
    ROLE,
    AVATAR_URL,
    USERNAME,
    EMAIL,
    DATE_MODIFIED,
    DATE_CREATED_GMT,
    IS_PAYING_CUSTOMER,
    DATE_MODIFIED_GMT,
    LAST_NAME,
    FIRST_NAME,
    DATE_CREATED,
    SHIPPING_POSTCODE,
    SHIPPING_ADDRESS_2,
    SHIPPING_ADDRESS_1,
    SHIPPING_COUNTRY,
    SHIPPING_CITY,
    SHIPPING_COMPANY,
    SHIPPING_LAST_NAME,
    SHIPPING_FIRST_NAME,
    SHIPPING_STATE,
    BILLING_POSTCODE,
    BILLING_ADDRESS_2,
    BILLING_ADDRESS_1,
    BILLING_COUNTRY,
    BILLING_CITY,
    BILLING_COMPANY,
    BILLING_EMAIL,
    BILLING_LAST_NAME,
    BILLING_FIRST_NAME,
    BILLING_PHONE,
    BILLING_STATE
FROM {{ ref('stg_customer_montem') }}
where not _FIVETRAN_DELETED

union ALL

SELECT 0 as customer_id,
    '99' as customer_key,
    NULL as ROLE,
    NULL as AVATAR_URL,
    'GUEST' as USERNAME,
    NULL as EMAIL,
    NULL as DATE_MODIFIED,
    NULL as DATE_CREATED_GMT,
    NULL as IS_PAYING_CUSTOMER,
    NULL as DATE_MODIFIED_GMT,
    NULL as LAST_NAME,
    'GUEST' as FIRST_NAME,
    NULL as DATE_CREATED,
    NULL as SHIPPING_POSTCODE,
    NULL as SHIPPING_ADDRESS_2,
    NULL as SHIPPING_ADDRESS_1,
    NULL as SHIPPING_COUNTRY,
    NULL as SHIPPING_CITY,
    NULL as SHIPPING_COMPANY,
    NULL as  SHIPPING_LAST_NAME,
    NULL as SHIPPING_FIRST_NAME,
    NULL as SHIPPING_STATE,
    NULL as BILLING_POSTCODE,
    NULL as BILLING_ADDRESS_2,
    NULL as BILLING_ADDRESS_1,
    NULL as BILLING_COUNTRY,
    NULL as BILLING_CITY,
    NULL as BILLING_COMPANY,
    NULL as BILLING_EMAIL,
    NULL as BILLING_LAST_NAME,
    NULL as BILLING_FIRST_NAME,
    NULL as BILLING_PHONE,
    NULL as BILLING_STATE

union all 

SELECT CUSTOMER_ID,
    {{ dbt_utils.generate_surrogate_key(['CUSTOMER_ID']) }} as customer_key,
    NULL as ROLE,
    NULL as AVATAR_URL,
    NULL as USERNAME,
    BILLING_EMAIL as EMAIL,
    NULL as DATE_MODIFIED,
    NULL as DATE_CREATED_GMT,
    NULL as IS_PAYING_CUSTOMER,
    NULL as DATE_MODIFIED_GMT,
    SHIPPING_LAST_NAME as LAST_NAME,
    SHIPPING_FIRST_NAME as FIRST_NAME,
    NULL as DATE_CREATED,
    SHIPPING_POSTCODE,
    SHIPPING_ADDRESS_2,
    SHIPPING_ADDRESS_1,
    SHIPPING_COUNTRY,
    SHIPPING_CITY,
    SHIPPING_COMPANY,
    SHIPPING_LAST_NAME,
    SHIPPING_FIRST_NAME,
    SHIPPING_STATE,
    BILLING_POSTCODE,
    BILLING_ADDRESS_2,
    BILLING_ADDRESS_1,
    BILLING_COUNTRY,
    BILLING_CITY,
    BILLING_COMPANY,
    BILLING_EMAIL,
    BILLING_LAST_NAME,
    BILLING_FIRST_NAME,
    BILLING_PHONE,
    BILLING_STATE
FROM {{ ref('int_customer') }}