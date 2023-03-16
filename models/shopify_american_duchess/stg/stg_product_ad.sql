with source as (
    select
        id::varchar as product_id,
        nullif(trim(lower(product_type)), '') as product_type,
        nullif(trim(lower(status)), '') as product_status,
        published_scope,
        nullif(template_suffix, '') as template_suffix,
        lower(vendor) as vendor,
        nullif(trim(lower(handle)), '') as product_handle,
        body_html,
        nullif(trim(lower(title)), '') as product_title,
        published_at as published_at_utc,
        created_at as created_at_utc,
        updated_at as updated_at_utc
        
    from {{ ref('snapshot_product_ad') }}
    where not _fivetran_deleted
        and dbt_valid_to is null
)
select * from source