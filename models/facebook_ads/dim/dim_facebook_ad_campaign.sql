select 
    {{ dbt_utils.generate_surrogate_key(['campaign_id']) }} as campaign_key,
    {{ dbt_utils.star(from=ref('stg_facebook_ads_campaign_history'), except=["is_most_recent_record"]) }}

from {{ ref('stg_facebook_ads_campaign_history') }}
where is_most_recent_record = 1