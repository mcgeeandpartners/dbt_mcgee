{{config(
    alias='dim_customer'
)}}

select
    c.id as customer_id,
    c.customer_key,
    c.first_name ||' '|| c.last_name as name,
    c.email,
    c.created_at,
    c.updated_at,
    lower(c.state) as customer_activity_state,
    c.tax_exempt as customer_is_tax_exempt,
    nullif(ca.phone, '') as customer_phone,
    nullif(ca.address_1, '') as customer_address_1,
    nullif(ca.address_2, '') as customer_address_2,
    nullif(ca.city, '') as customer_city,
    nullif(ca.country, '') as customer_country,
    nullif(ca.province, '') as customer_state,
    nullif(ca.zip, '') as customer_zipcode,
    nullif(ca.latitude, '') as customer_latitude,
    nullif(ca.longitude, '') as customer_longitude
from {{ ref('stg_customer_tenkara_usa') }} as c 
left join {{ref('stg_customer_address_tenkara_usa')}} as ca
    on c.id = ca.customer_id 
where ca.latest_entry = 1