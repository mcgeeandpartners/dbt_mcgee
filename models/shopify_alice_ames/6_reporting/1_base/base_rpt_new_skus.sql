--Variant names that havent been added to the sku list yet

{{
  config(
    materialized = 'view'
    )
}}

select 
    a.product_variant_name, a.product_title, min(c.order_placed_at_utc) as initial_order_placed_at_utc
from {{ ref('transform_order_line_item_alice_ames') }} as a 
inner join {{ ref('stg_order_alice_ames') }} as c 
    on a.order_id = c.order_id    
left join {{ ref('stg_master_sku_list_alice_ames') }} as b 
    on a.product_variant_name = b.product_variant_name
where b.product_variant_name is null
group by 1, 2