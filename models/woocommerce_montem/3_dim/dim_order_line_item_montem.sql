{{config(
    alias='dim_order_line_item'
)}}

select
    id,
    null as ofl_id,
    total,
    sku,
    variation_id,
    tax_class,
    name,
    quantity,
    subtotal,
    price,
    subtotal_tax,
    total_tax,
    order_id,
    product_id::varchar as product_id
from {{ ref("stg_order_line_item_montem") }}
where product_id != 0

union all

select
    id,
    null as ofl_id,
    total,
    sku,
    variation_id,
    tax_class,
    name,
    quantity,
    subtotal,
    price,
    subtotal_tax,
    total_tax,
    order_id,
    ip.product_id::varchar as product_id
from {{ ref("stg_order_line_item_montem") }} sto
left join {{ ref("int_products") }} ip on sto.name = ip.product_title
where sto.product_id = 0

union all

select
    null as id,
    ofl_id,
    total,
    null as sku,
    null as variation_id,
    null as tax_class,
    null as name,
    quantity,
    subtotal,
    price,
    null as subtotal_tax,
    null as total_tax,
    order_id,
    null as product_id
from {{ ref("stg_order_fee_line_montem") }} ofe
