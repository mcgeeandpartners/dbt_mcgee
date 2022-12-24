--Performance of an ad for each day broken down by ad_network_type and device.

select 
    ad_key, 
    ad_g.ad_group_key, 
    ad_c.campaign_key, 
    ad_a.account_key,
    date_key, 
    dga.date,
    ad_s.ad_network_type,
    ad_s.device,
    ad_s.clicks, 
    ad_s.spend, 
    ad_s.impressions
from {{ ref('stg_google_ads_ad_stats') }} as ad_s 
inner join {{ ref('dim_google_ad') }} as ad on ad_s.ad_id = ad.ad_id
left join {{ ref('dim_date_google_ads') }} as dga on ad_s.date_day = dga.date
left join {{ ref('dim_google_ad_group') }} as ad_g on ad_s.ad_group_id = ad_g.ad_group_id
left join {{ ref('dim_google_ad_campaign') }} as ad_c on ad_s.campaign_id = ad_c.campaign_id
left join {{ ref('dim_google_ad_account') }} as ad_a on ad_s.account_id = ad_a.account_id