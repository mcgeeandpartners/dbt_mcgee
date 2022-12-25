with source as (

    select * 
    from {{ source('facebook_ads', 'ad_history') }}
),

final as (
    
    select 
        id as ad_id,
        name as ad_name,
        status as ad_status, 
        bid_type,
        account_id,
        ad_set_id,   
        campaign_id,
        creative_id,
        created_time as created_at,
        updated_time as updated_at,
        row_number() over (partition by id order by updated_time desc) = 1 as is_most_recent_record
    from source
)

select * 
from final
