{{ config(alias="fact_order_line_item") }}

select 
    d.date_key,
    c.customer_key,
    p.product_key,
    {# discount.discount_key, #} --Will add after investigating the discount application table. Per Caroline, this can be tabled for later
    o.order_id,
    oli.order_line_item_id,
    o.cancel_reason as order_cancel_reason,
    o.financial_status as order_financial_status,
    o.fulfillment_status as order_fulfillment_status,
    o.currency as order_currency,
    o.taxes_included as order_has_tax_included,
    iff(date_trunc('day', o.order_placed_at_utc) = date_trunc('day', c.customer_created_at), 1, 0)::boolean as is_new_customer_order,
    
    o.total_price as order_total_price,
    o.subtotal_price as order_subtotal_price,
    o.total_tax as order_total_tax,
    round(o.total_price - o.subtotal_price - o.total_tax, 2) as order_shipping_price,
    o.total_discounts as order_total_discount,
    o.current_total_price as order_current_total_price,
    o.current_subtotal_price as order_current_subtotal_price,
    o.current_total_tax as order_current_total_tax,
    o.current_total_discounts as order_current_total_discount,
    o.total_line_items_price as order_total_line_items_price,
    o.revenue_shipping as order_revenue_shipping,
    o.refund_total as order_refund_total,

--
    oli.vendor,
    iff(is_vendor_route = false, oli.quantity, 0) as units_sold_product, --units_sold_ex_route
    iff(is_vendor_route = true, oli.quantity, 0) as units_sold_route, --units_sold_route
    iff(is_vendor_route = false, oli.quantity * oli.price, 0) as gross_revenue_product, --total_line_item_price_ex_route
    iff(is_vendor_route = true, oli.quantity * oli.price, 0) as gross_revenue_route, --total_line_item_price_route
    oli.quantity as units_sold,
    oli.price as order_line_item_price,
    oli.price * oli.quantity as gross_revenue_total, -- total_line_item_price
    nvl(i.msrp, 0) as msrp_product,
    iff(is_vendor_route = false, greatest(oli.price, msrp_product) * oli.quantity, 0) as gross_revenue_adj_product, --total_line_item_price_adj_ex_route
    greatest(oli.price, msrp_product) * oli.quantity as gross_revenue_adj,
    (greatest(oli.price, msrp_product) - oli.price) * oli.quantity as total_line_item_adj_implied_discount,

    {# //, sum(iff(ol.vendor <> 'Route', ol.total_discount,0)) as total_line_item_discount_ex_route
    //, sum(ol.total_discount) as total_line_item_discount #}

    'alice_ames' as sub_company 

from {{ref('stg_order_alice_ames')}} as o 
left join {{ref('stg_order_line_item_alice_ames')}} as oli 
    on o.order_id = oli.order_id
left join {{ref('dim_product_alice_ames')}} as p 
    on oli.product_id = p.product_id
    and oli.product_variant_id = p.product_variant_id
left join {{ref('dim_customer_alice_ames')}} as c 
    on o.customer_id = c.customer_id
left join {{ref('dim_date_alice_ames')}} as d 
    on d."DATE" = o.order_placed_at_utc::date
{# left join {{ref('dim_discount_alice_ames')}} as discount 
    on discount.order_id = o.order_id #}
left join {{ ref('int_historic_msrp') }} as i 
    on oli.product_title = i.product_title
    and o.order_placed_at_utc::date = i.order_date