select 
    {{ dbt_utils.surrogate_key(['campaign_id']) }} as campaign_key,
    campaign_id,
    campaign_name, 
    advertising_channel_type, 
    advertising_channel_subtype, 
    start_date, 
    end_date, 
    serving_status, 
    status, 
    tracking_url_template,
    updated_at
from {{ ref('stg_google_ads_campaign_history') }}
where is_most_recent_record = 1