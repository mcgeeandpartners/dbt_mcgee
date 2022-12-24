with source as (

    select * 
    from {{ source('google_ads', 'ad_group_history') }}

),

final as (

    select 
        cast(id as {{ dbt_utils.type_string() }}) as ad_group_id,
        updated_at::timestamp_ntz as updated_at,
        type as ad_group_type, 
        campaign_id, 
        campaign_name, 
        name as ad_group_name, 
        status as ad_group_status,
        row_number() over (partition by id order by updated_at::timestamp_ntz desc) = 1 as is_most_recent_record
    from source

)

select * 
from final