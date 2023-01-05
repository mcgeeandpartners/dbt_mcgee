with source as (

    select * 
    from {{ source('google_ads', 'ad_group_criterion_history') }}

),

final as (

    select 
        id as criterion_id,
        ad_group_id,
        updated_at::timestamp_ntz as updated_at,
        type,
        status,
        keyword_match_type,
        keyword_text,
        row_number() over (partition by id order by updated_at::timestamp_ntz desc) = 1 as is_most_recent_record
    from source

)

select * 
from final