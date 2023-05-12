select 
    {{ dbt_utils.generate_surrogate_key(['ad_set_id']) }} as ad_set_key,
    {{ dbt_utils.star(from=ref('stg_facebook_ads_ad_set_history'), except=["is_most_recent_record"]) }}
from {{ ref('stg_facebook_ads_ad_set_history') }}
where is_most_recent_record = 1