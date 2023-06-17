{{ config(alias="dim_products") }}

select id,
    {{ dbt_utils.surrogate_key(['id']) }} as product_key,
    virtual,
    kit_build,
    created_at,
    needs_serial_number,
    updated_at,
    tariff_code,
    kit,
    legacy_id,
    ignore_on_invoice,
    sku,
    barcode,
    large_thumbnail,
    thumbnail,
    ignore_on_customs,
    active,
    dropship,
    no_air,
    country_of_manufacture,
    account_id,
    not_owned,
    customs_value,
    name,
    final_sale,
    images,
    tags,
    kit_components,
    dimensions
from {{ ref("stg_shiphero_products") }}

