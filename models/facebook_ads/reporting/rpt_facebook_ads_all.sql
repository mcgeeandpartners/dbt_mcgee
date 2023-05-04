SELECT 
    fact_ads.full_date as reporting_date
    , act.account_id
    , act.account_name
    , act.account_status as account_current_status
    , act.currency as account_currency
    , act.timezone_name as account_timezone
    , campaign.campaign_id
    , campaign.source_campaign_id
    , campaign.campaign_name
    , campaign.start_at as campaign_start_at
    , campaign.end_at as campaign_end_at
    , campaign.status as campaign_status
    , campaign.daily_budget as campaign_daily_budget
    , campaign.lifetime_budget as campaign_lifetime_budget
    , campaign.budget_remaining as campaign_budget_remaining
    , campaign.bid_strategy as campaign_bid_strategy
    , campaign.objective as campaign_objective
    , campaign.smart_promotion_type as campaign_smart_promotion_type
    , campaign.special_ad_category as campaign_speciaL_ad_category
    , campaign.pacing_type as campaign_pacing_type
    , campaign.updated_at as campaign_updated_at
    , campaign.created_at as campaign_created_at
    , ad_set.ad_set_id
    , ad_set.ad_set_name
    , ad_set.status as ad_set_status
    , ad_set.bid_strategy as ad_set_bid_strategy
    , ad_set.daily_budget as ad_set_daily_budget
    , ad_set.budget_remaining as ad_set_budget_remaining
    , ad_set.targeting_age_min as ad_set_targeting_age_min
    , ad_set.targeting_age_max as ad_set_targeting_age_max
    , ad_set.start_at as ad_set_start_at
    , ad_set.end_at as ad_set_end_at
    , ad_set.updated_at as ad_set_updated_at
    , ads.ad_id
    , ads.ad_name
    , ads.ad_status
    , ads.bid_type as ad_bid_type
    , ads.updated_at as ad_updated_at
    , ads.created_at as ad_created_at
    , fact_ads.impressions
    , fact_ads.clicks
    , fact_ads.spend
    , fact_ads.reach
FROM {{ ref('fact_facebook_ad') }} as fact_ads 
LEFT JOIN {{ ref('dim_facebook_ad_account') }} as act
    ON fact_ads.account_key = act.account_key
LEFT JOIN {{ ref('dim_facebook_ad_campaign') }} as campaign
    ON fact_ads.campaign_key = campaign.campaign_key
LEFT JOIN {{ ref('dim_facebook_ad_set') }} as ad_set
    ON fact_ads.ad_set_key = ad_set.ad_set_key
LEFT JOIN {{ ref('dim_facebook_ad') }} as ads
    ON fact_ads.ad_key = ads.ad_key