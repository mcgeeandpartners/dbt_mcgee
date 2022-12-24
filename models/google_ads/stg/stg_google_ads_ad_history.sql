with source as (

    select * 
    from {{ source('google_ads', 'ad_history') }}

),

final as (
    
    select 
        cast(ad_group_id as {{ dbt_utils.type_string() }}) as ad_group_id, 
        id as ad_id,
        name as ad_name,
        updated_at::timestamp_ntz as updated_at,
        type as ad_type,
        status as ad_status,
        display_url,
        final_urls as source_final_urls,
        replace(replace(final_urls, '[', ''),']','') as final_urls,
        row_number() over (partition by id, ad_group_id order by updated_at::timestamp_ntz desc) = 1 as is_most_recent_record
    from source
),

final_urls as (

    select 
        *,
        --Extract the first url within the list of urls provided within the final_urls field
        {{ dbt_utils.split_part(string_text='final_urls', delimiter_text="','", part_number=1) }} as final_url

    from final

),

url_fields as (
    select 
        *,
        {{ dbt_utils.split_part('final_url', "'?'", 1) }} as base_url,
        {{ dbt_utils.get_url_host('final_url') }} as url_host,
        '/' || {{ dbt_utils.get_url_path('final_url') }} as url_path,
        {{ dbt_utils.get_url_parameter('final_url', 'utm_source') }} as utm_source,
        {{ dbt_utils.get_url_parameter('final_url', 'utm_medium') }} as utm_medium,
        {{ dbt_utils.get_url_parameter('final_url', 'utm_campaign') }} as utm_campaign,
        {{ dbt_utils.get_url_parameter('final_url', 'utm_content') }} as utm_content,
        {{ dbt_utils.get_url_parameter('final_url', 'utm_term') }} as utm_term
    from final_urls
)

select * 
from url_fields