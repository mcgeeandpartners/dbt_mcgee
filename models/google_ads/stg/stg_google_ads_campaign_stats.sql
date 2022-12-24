with source as (

    select * 
    from {{ source('google_ads', 'campaign_stats') }}

),

final as (
    
    select 
        customer_id as account_id, 
        date as date_day, 
        id as campaign_id, 
        ad_network_type,
        device,
        clicks, 
        cost_micros / 1000000.0 as spend, 
        impressions    
    from source

)

select * 
from final