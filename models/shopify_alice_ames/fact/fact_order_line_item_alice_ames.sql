{{ config(alias="fact_order_line_item") }}

select 
    d.date_key,
    c.customer_key,
    coalesce(p.product_key, p2.product_key) as product_key,
    {# discount.discount_key, #} --Will add after investigating the discount application table. Per Caroline, this can be tabled for later
    o.order_id,
    oli.order_line_item_id,
    o.order_cancel_reason,
    o.order_financial_status,
    o.order_fulfillment_status,
    o.currency as order_currency,
    o.taxes_included as order_has_tax_included,
    iff(date_trunc('day', o.order_placed_at_utc) = date_trunc('day', c.customer_created_at), 1, 0)::boolean as is_new_customer_order,
--Aggregations
    sum(order_line_item_units_product) over (partition by o.order_id) as order_units_product,
    sum(order_line_item_units_route) over (partition by o.order_id) as order_units_route,
    max(iff(is_vendor_route = false, i.msrp, 0)) over (partition by o.order_id) * order_units_product as order_gross_revenue_product, --msrp is on the product level so we take the max
    sum(oli.order_line_item_price_route) over (partition by o.order_id) * order_units_route as order_gross_revenue_route,
    o.total_tax as order_gross_revenue_tax,
    round(o.total_price - o.subtotal_price - o.total_tax, 2) as order_gross_revenue_shipping,
    order_gross_revenue_product + order_gross_revenue_route + order_gross_revenue_tax + order_gross_revenue_shipping as order_gross_revenue_total,
    o.total_discounts + sum((nvl(i.msrp, oli.order_line_item_price) - oli.order_line_item_price) * oli.order_line_item_units) over (partition by o.order_id) as order_discount,
    round(o.total_price - o.current_total_price, 2) as order_refund,

    --we might be missing the order shipping discount. This can be calculated. 

    order_gross_revenue_total - order_discount - order_refund as order_net_revenue_total,
    oli.order_line_item_vendor,
    nvl(i.msrp, oli.order_line_item_price) as order_line_item_msrp,
    oli.order_line_item_units,
    order_line_item_msrp * oli.order_line_item_units as order_line_item_gross_revenue,
    ((order_line_item_msrp - oli.order_line_item_price) * oli.order_line_item_units) + oli.total_discount as order_line_item_total_discount,    
    nvl(oli.sku, p.product_sku) as order_line_item_sku,
    p.product_barcode,
    oli.is_gift_card as order_line_item_is_gift_card,
    oli.is_taxable as order_line_item_is_taxable

    {# //, sum(iff(ol.vendor <> 'Route', ol.total_discount,0)) as total_line_item_discount_ex_route
    //, sum(ol.total_discount) as total_line_item_discount #}

from {{ref('stg_order_alice_ames')}} as o 
left join {{ref('stg_order_line_item_alice_ames')}} as oli 
    on o.order_id = oli.order_id
--this join is for the regular not null product ids that exist in both order line item and product tables
left join {{ref('dim_product_alice_ames')}} as p 
    on oli.product_id = p.product_id
    and oli.product_variant_id = p.product_variant_id
--this join is for the null product ids which we have put the logic to backfill
left join {{ref('dim_product_alice_ames')}} as p2 
    on oli.product_id = p2.product_id
    and p2.product_status = 'deleted' --only the ones from "transform_null_product_backfill_alice_ames" table
left join {{ref('dim_customer_alice_ames')}} as c 
    on o.customer_id = c.customer_id
left join {{ref('dim_date_alice_ames')}} as d 
    on d."DATE" = o.order_placed_at_utc::date
{# left join {{ref('dim_discount_alice_ames')}} as discount 
    on discount.order_id = o.order_id #}
left join {{ ref('int_historic_msrp') }} as i 
    on oli.product_title = i.product_title
    and o.order_placed_at_utc::date = i.order_date