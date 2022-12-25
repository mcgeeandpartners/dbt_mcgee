with source as (

    select * 
    from {{ source('google_ads', 'keyword_stats') }}

),

final as (

    select 
        _fivetran_id as keyword_id,
        customer_id as account_id, 
        date as date_day, 
        ad_group_id,
        ad_group_criterion_criterion_id as criterion_id,
        campaign_id, 
        ad_network_type,
        device,
        clicks, 
        cost_micros / 1000000.0 as spend, 
        impressions
    from source

)

select * 
from final