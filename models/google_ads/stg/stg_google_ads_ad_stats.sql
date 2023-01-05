with source as (

    select * 
    from {{ source('google_ads', 'ad_stats') }}

),

final as (

    
    select 
        customer_id as account_id, 
        date as date_day, 
        ad_group_id,
        ad_network_type,
        device,
        ad_id, 
        campaign_id, 
        clicks, 
        cost_micros / 1000000.0 as spend, 
        impressions
    from source

)

select * 
from final