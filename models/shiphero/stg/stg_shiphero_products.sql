{{ config(alias="stg_products") }}

with base as (
    select
        virtual,
        kit_build,
        created_at,
        needs_serial_number,
        updated_at,
        tariff_code,
        kit,
        legacy_id,
        id,
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
        _fivetran_synced,
        images,
        tags,
        warehouse_products,
        kit_components,
        dimensions,
        row_number() over (
            partition by id order by _fivetran_synced desc
        ) as ranking
    from {{ source("shiphero", "products") }}
    )
select *
from base
where ranking = 1
