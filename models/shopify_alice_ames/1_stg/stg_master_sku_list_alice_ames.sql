--Staging model for master sku list uploaded via Fivetran gsheet connector

select
    {{ dbt_utils.surrogate_key(['product_variant_id', 'product_id', 'product_title']) }} as sku_list_key, --5 cases that makes this not unique. We exclude them while testing
    _row::varchar as row_sequence,
    lower(trim(product_id))::varchar as product_id,
    lower(trim(product_title))::varchar as product_title,
    lower(trim(product_variant_id))::varchar as product_variant_id,
    lower(trim(product_variant_name))::varchar as product_variant_name,
    {{ standardize_column_values('product_category') }},
    {{ standardize_column_values('product_type') }},
    {{ standardize_column_values('product_sub_type') }},
    {{ standardize_column_values('product_campaign') }},
    {{ standardize_column_values('campaign_year') }},
    ifnull(print_t_f_, 0)::boolean as is_print,
    {{ standardize_column_values('print_solid_name') }},
    {{ standardize_column_values('base_fabric_color') }},
    {{ standardize_column_values('made_in') }},
    {{ standardize_column_values('manufacturer') }},
    _fivetran_synced

from {{ source('aestuary_gheets', 'sku_list_alice_ames') }}