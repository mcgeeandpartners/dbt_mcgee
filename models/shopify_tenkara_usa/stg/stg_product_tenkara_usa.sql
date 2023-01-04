select
    id,
    {{ dbt_utils.surrogate_key(['id']) }} as product_key,
    product_type,
    status,
    published_scope,
    template_suffix,
    vendor,
    handle,
    body_html,
    title,
    published_at,
    created_at,
    updated_at
from {{ source('shopify_tenkara_usa', 'product') }}
where not _fivetran_deleted
