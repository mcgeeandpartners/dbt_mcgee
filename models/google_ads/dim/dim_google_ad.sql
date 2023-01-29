select 
    {{ dbt_utils.surrogate_key(['ad_id', 'ad_group_id']) }} as ad_key,
    ad_id,
    ad_name, 
    ad_type,
    ad_status,
    display_url,
    source_final_urls,
    final_urls,
    final_url,
    base_url,
    url_host,
    url_path,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term,
    updated_at
from {{ ref('stg_google_ads_ad_history') }}
where is_most_recent_record = 1