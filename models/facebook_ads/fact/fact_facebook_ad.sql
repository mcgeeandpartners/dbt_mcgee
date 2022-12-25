select    
    d.date_key,
    ba.date_day as full_date,
    fa.account_key, 
    fc.campaign_key,
    fas.ad_set_key,
    dfa.ad_key,
    ba.impressions,
    ba.clicks,
    ba.spend,
    ba.reach

from {{ ref('stg_facebook_ads_basic_ad') }} as ba 
left join {{ ref('dim_facebook_ad_date') }} as d 
    on ba.date_day = d.date
left join {{ ref('stg_facebook_ads_ad_history') }} as ah 
    on ba.ad_id = ah.ad_id
    and is_most_recent_record = 1
left join {{ ref('dim_facebook_ad') }} as dfa 
    on ah.ad_id = dfa.ad_id
left join {{ ref('dim_facebook_ad_set') }} as fas 
    on ah.ad_set_id = fas.ad_set_id
left join {{ ref('dim_facebook_ad_campaign') }} as fc 
    on ah.campaign_id = fc.campaign_id
left join {{ ref('dim_facebook_ad_account') }} as fa 
    on ah.account_id = fa.account_id