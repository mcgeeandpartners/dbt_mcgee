{{ config(alias="fact_order_line_item") }}

select 
    d.date_key,
    c.customer_key,
    p.product_key,
    discount.discount_key,
    o.order_id,
    oli.id as order_line_item_id,
    o.cancel_reason as order_cancel_reason,
    o.financial_status as order_financial_status,
    o.currency as order_currency,
    o.taxes_included as order_taxes_included,
    o.total_price as order_total_price,
    o.total_line_items_price as order_total_line_items_price,
    o.discounts_total as order_discounts_total,
    o.total_tax as order_total_tax,
    o.revenue_shipping as order_revenue_shipping,
    o.refund_total as order_refund_total,
    oli.vendor,
    case 
        when coalesce(oli.vendor, '') <> 'Route' then FALSE
        when coalesce(oli.vendor, '') = 'Route' then TRUE
        else NULL
    end as is_route,
    case 
        when is_route = false then coalesce(oli.quantity, 0)
        else 0 
    end as units_sold_product,
    case 
        when is_route = true then coalesce(oli.quantity, 0)
        else 0 
    end as units_sold_route,
    oli.price as order_line_price,
    oli.quantity as order_line_quantity,
    case 
        when is_route = false then coalesce(oli.quantity* oli.price, 0)
        else 0 
    end as revenue_product,
    case 
        when is_route = true then coalesce(oli.quantity* oli.price, 0)
        else 0 
    end as revenue_route,
    'american_duchess' as sub_company 
from {{ref('stg_order_ad')}} o 
left join {{ref('stg_order_line_item_ad')}} oli 
on o.order_id = oli.order_id
left join {{ref('dim_product_ad')}} p 
on oli.product_id = p.product_id
and oli.variant_id = p.product_variant_id
left join {{ref('dim_customer_ad')}} c 
on o.customer_id = c.customer_id
left join {{ref('dim_date_ad')}} d 
on d."DATE" = o.order_placed_utc::date
left join {{ref('dim_discount_ad')}} discount 
on discount.order_id = o.order_id