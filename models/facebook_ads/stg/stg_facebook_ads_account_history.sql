with source as (

    select * 
    from {{ source('facebook_ads', 'account_history') }}
),

final as (
    
    select 
        id as account_id,
        name as account_name,
        account_status,
        business_street,
        business_street_2,
        business_city,
        business_state,
        business_zip,
        business_country_code,
        created_time as created_at,
        currency,
        tax_id_status,
        timezone_name,
        _fivetran_synced,
        row_number() over (partition by id order by _fivetran_synced desc) = 1 as is_most_recent_record
    from source

)

select * 
from final