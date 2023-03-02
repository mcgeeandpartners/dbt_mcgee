select
	lower(trim(title)) as product_title, 
    lower(trim(name)) as product_variant_name,
    lower(trim(product_category)) as product_category,
    lower(trim(product_sub_type)) as product_sub_type,
    lower(trim(product_campaign)) as product_campaign,
    campaign_year::varchar as campaign_year,
    ifnull(is_print, 'false')::boolean as is_print,
    lower(trim(print_solid_name)) as print_solid_name,
    nullif(lower(trim(base_fabric_color)), '0') as base_fabric_color, --treating 0 as nulls to remove dupes
    lower(trim(made_in)) as made_in,
    lower(trim(manufacturer)) as manufacturer 
from {{ source('seeds', 'master_sku_list_alice_ames') }}
qualify row_number() over (partition by product_variant_name order by is_print) = 1