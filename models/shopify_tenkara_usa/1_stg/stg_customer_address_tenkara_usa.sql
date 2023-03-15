select 
    id as customer_address_id,
    customer_id,
    nullif(first_name, '') as first_name,
    nullif(last_name, '') as last_name,
    nullif(name, '') as full_name,
    nullif(address_1, '') as customer_address_1,
    nullif(address_2, '') as customer_address_2,
    nullif(city, '') as customer_city,
    nullif(zip, '') as customer_zipcode,
    nullif(province, '') as customer_state,
    nullif(country, '') as customer_country,
    nullif(country_code, '') as country_code,
    nullif(longitude, '') as customer_longitude,
    nullif(latitude, '') as customer_latitude,
    nullif(company, '') as company,
    nullif(phone, '') as customer_phone,
    nullif(province_code, '') as province_code,
    is_default,
    row_number() over (partition by customer_id order by customer_address_id desc) as latest_entry

from {{source('shopify_tenkara_usa', 'customer_address')}}