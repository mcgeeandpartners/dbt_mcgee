select 
    {{ dbt_utils.surrogate_key(['ad_group_id']) }} as ad_group_key,
    ad_group_id,
    ad_group_type, 
    ad_group_name, 
    ad_group_status,
    updated_at
from {{ ref('stg_google_ads_ad_group_history') }}
where is_most_recent_record = 1