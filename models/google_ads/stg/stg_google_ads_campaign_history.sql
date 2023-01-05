with source as (

    select * 
    from {{ source('google_ads', 'campaign_history') }}

),

final as (
    
    select 
        id as campaign_id, 
        updated_at::timestamp_ntz as updated_at,
        name as campaign_name,
        customer_id as account_id,
        advertising_channel_type,
        advertising_channel_subtype,
        start_date,
        end_date,
        serving_status,
        status,
        tracking_url_template,
        row_number() over (partition by id order by updated_at::timestamp_ntz desc) = 1 as is_most_recent_record
    from source

)

select * 
from final