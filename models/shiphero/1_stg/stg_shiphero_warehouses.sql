{{ config(alias="stg_warehouses") }}

select 'V2FyZWhvdXNlOjU4MjI2' as id,
    58226 as legacy_id,
    'Shipping Department - Primary' as address_name,
    '2575 W 400 N Ste 100' as address1,
    NULL as address2,
    'Lindon' as city,
    'UT' as state,
    'US' as country,
    '84042' as zip,
    '801-438-7850' as phone,
    'QWNjb3VudDo1MDgzNA==' as account_id,
    'Primary' as identifier,
    false as dynamic_slotting,
    'devin@moorfulfillment.com' as invoice_email,
    'default' as profile

union ALL

select 'V2FyZWhvdXNlOjgwMjkx' as id,
    80291 as legacy_id,
    'Alice + Ames' as address_name,
    '2575 W 400 N' as address1,
    'Suite 100' as address2,
    'Lindon' as city,
    'UT' as state,
    'US' as country,
    '84042' as zip,
    '14158940835' as phone,
    'QWNjb3VudDo2NTg1Mg==' as account_id,
    'Primary' as identifier,
    false as dynamic_slotting,
    'help@aliceandames.com' as invoice_email,
    'default' as profile

