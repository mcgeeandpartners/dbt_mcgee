with source as (

    select * 
    from {{ source('facebook_ads', 'ad_set_history') }}
),

final as (
    
    select 
        id as ad_set_id,
        name as ad_set_name,
        account_id,
        campaign_id,
        status,
        bid_strategy,
        daily_budget,
        budget_remaining,
        targeting_age_min,
        targeting_age_max,
        start_time as start_at,
        end_time as end_at,
        updated_time as updated_at,
        row_number() over (partition by id order by updated_time desc) = 1 as is_most_recent_record
    from source

)

select * 
from final
