select 
    {{ dbt_utils.surrogate_key(['ad_id']) }} as ad_key,
    ad_id,
    ad_name, 
    ad_status,
    bid_type,
    updated_at,
    created_at
from {{ ref('stg_facebook_ads_ad_history') }}
where is_most_recent_record = 1