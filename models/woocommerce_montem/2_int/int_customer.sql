-- Get customer_ids that are part of orders, but not in customer table.

select CUSTOMER_ID,
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
from {{ref('stg_order_montem')}}
where customer_id not in (
    select distinct id 
    from {{ref('stg_customer_montem')}}
)
and customer_id != 0 --Also exclude guest users
