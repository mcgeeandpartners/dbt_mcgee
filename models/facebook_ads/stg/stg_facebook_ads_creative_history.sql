with source as (

    select * 
    from {{ source('facebook_ads', 'creative_history') }}
),

final as (
    
    select 
        id as creative_id,
        account_id,
        name as creative_name,
        _fivetran_id,
        page_link,
        template_page_link,
        url_tags,
        asset_feed_spec_link_urls,
        object_story_link_data_child_attachments,
        object_story_link_data_caption, 
        object_story_link_data_description, 
        object_story_link_data_link, 
        object_story_link_data_message,
        template_app_link_spec_ios,
        template_app_link_spec_ipad,
        template_app_link_spec_android,
        template_app_link_spec_iphone,
        _fivetran_synced,
        row_number() over (partition by id order by _fivetran_synced desc) = 1 as is_most_recent_record
    from source
)

select * 
from final
