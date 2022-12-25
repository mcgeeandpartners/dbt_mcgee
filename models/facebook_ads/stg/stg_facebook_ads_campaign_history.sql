with source as (

    select * 
    from {{ source('facebook_ads', 'campaign_history') }}
),

final as (
    
    select 
        id as campaign_id,
        account_id,
        source_campaign_id,
        name as campaign_name,
        start_time as start_at,
        stop_time as end_at,
        status,
        daily_budget,
        lifetime_budget,
        budget_remaining,
        bid_strategy,
        objective,
        smart_promotion_type,
        special_ad_category,
        pacing_type,
        updated_time as updated_at,
        created_time as created_at,
        row_number() over (partition by id order by updated_time desc) = 1 as is_most_recent_record
    from source

)

select * 
from final
