--Variant names that havent been added to the sku list yet

{{
  config(
    materialized = 'view',
    )
}}

select distinct a.product_variant_name, a.product_title
from {{ ref('transform_order_line_item_alice_ames') }} as a 
left join {{ ref('stg_master_sku_list_alice_ames') }} as b 
    on a.product_variant_name = b.product_variant_name
where b.product_variant_name is null