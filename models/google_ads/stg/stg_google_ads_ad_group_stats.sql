with source as (

    select * 
    from {{ source('google_ads', 'ad_group_stats') }}

),

final as (

    select
        id as ad_group_id, 
        customer_id as account_id, 
        date as date_day, 
        campaign_id, 
        device,
        ad_network_type,
        clicks, 
        cost_micros / 1000000.0 as spend, 
        impressions
    from source

)

select * 
from final