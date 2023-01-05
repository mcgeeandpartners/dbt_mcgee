with source as (

    select * 
    from {{ source('google_ads', 'account_history') }}

),

final as (
    
    select 
        id as account_id,
        updated_at::timestamp_ntz as updated_at,
        currency_code,
        auto_tagging_enabled,
        time_zone,
        descriptive_name as account_name,
        row_number() over (partition by id order by updated_at::timestamp_ntz desc) = 1 as is_most_recent_record
    from source
)

select * 
from final