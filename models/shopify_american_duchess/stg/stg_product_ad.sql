select
    created_at,
    product_type,
    status,
    published_scope,
    template_suffix,
    vendor,
    handle,
    id,
    {{ dbt_utils.surrogate_key(['id']) }} as product_key,
    body_html,
    title,
    published_at,
    updated_at
from {{ source("SHOPIFY_AMERICAN_DUCHESS", "PRODUCT") }}
where not _fivetran_deleted
