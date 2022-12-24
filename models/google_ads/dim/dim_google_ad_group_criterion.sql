select 
    {{ dbt_utils.surrogate_key(['criterion_id']) }} as criterion_key,
    criterion_id,
    type,
    status,
    keyword_match_type,
    keyword_text,
    updated_at
from {{ ref('stg_google_ads_ad_group_criterion_history') }}
where is_most_recent_record = 1