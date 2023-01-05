with source as (

    select * 
    from {{ source('facebook_ads', 'basic_ad') }}
),

final as (
    
    select 
        ad_id,
        ad_name,
        adset_name as ad_set_name,
        date as date_day,
        account_id,
        impressions,
        coalesce(inline_link_clicks,0) as clicks,
        spend,
        reach,
        frequency

    from source
)

select * 
from final
