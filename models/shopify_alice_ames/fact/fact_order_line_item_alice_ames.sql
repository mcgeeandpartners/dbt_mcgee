{{ config(alias="fact_order_line_item") }}

select 
    d.date_key,
    c.customer_key,
    p.product_key,
    {# discount.discount_key, #} --Will add after investigating the discount application table. Per Caroline, this can be tabled for later
    o.order_id,
    oli.order_line_item_id,
    o.order_cancel_reason,
    o.order_financial_status,
    o.order_fulfillment_status,
    o.currency as order_currency,
    o.taxes_included as order_has_tax_included,
    iff(date_trunc('day', o.order_placed_at_utc) = date_trunc('day', c.customer_created_at), 1, 0)::boolean as is_new_customer_order,

--Caroline suggestions
    sum(order_line_item_units_product) over (partition by o.order_id) as order_units_product,
    sum(order_line_item_units_route) over (partition by o.order_id) as order_units_route,
    
    {# ADD order_gross_revenue_product (this should be the sum of the adjusted MSRP of an order, excluding Vendor = 'Route') #}
    max(iff(is_vendor_route = false, i.msrp, 0)) over (partition by o.order_id) * order_units_product as order_gross_revenue_product, --msrp is on the product level so we take the max

    {# ADD order_gross_revenue_route (sum of Route price on an order, there should only be one per order but just in case I would do sum(price when vendor = 'Route')) #}
    sum(oli.order_line_item_price_route) over (partition by o.order_id) * order_units_route as order_gross_revenue_route,

    o.total_tax as order_gross_revenue_tax,
    round(o.total_price - o.subtotal_price - o.total_tax, 2) as order_gross_revenue_shipping,

    {# order_total_price >> order_gross_revenue_total (this should = the sum of the 4 lines above) #}
    order_gross_revenue_product + order_gross_revenue_route + order_gross_revenue_tax + order_gross_revenue_shipping as order_gross_revenue_total,
    
    {# order_total_discount >> order_discounts (this needs to include the "total_line_item_adj_implied_discount" for all line items in the order) #}
    o.total_discounts + sum((nvl(i.msrp, oli.order_line_item_price) - oli.order_line_item_price) * oli.order_line_item_units) over (partition by o.order_id) as order_discount,
    
    {# order_total_price - order_current_total_price >> order_refunds (this may be labeled Order_refund_total now) #}
    round(order_gross_revenue_total - o.current_total_price, 2) as order_refund,

    --we might be missing the order shipping discount. This can be calculated. 

    {# order_net_revenue_total (this = order_gross_revenue_total - order_discounts - order_refunds) #}
    order_gross_revenue_total - order_discount - order_refund as order_net_revenue_total,

    oli.order_line_item_vendor,
    {# MSRP_product >> Order_line_item_msrp (this looks correct for products, can you make sure it also shows the price if the Vendor = 'Route' i.e. this should never be 0) #}
    nvl(i.msrp, oli.order_line_item_price) as order_line_item_msrp,
    oli.order_line_item_units,

    {# max(gross_revenue_adj_product, gross_revenue_route) >> order_line_item_gross_revenue (i basically want to collapse Product and Route revenue fields into a single field at the order_line level, we only need that distinction at the order level) #}
    greatest(order_gross_revenue_product, order_gross_revenue_route) as order_line_item_gross_revenue,
    
    {# total_line_item_adj_implied_discount + order_line.total_discount >> order_line_item_total_discount (**please make sure this includes not only the implied discount, but ALSO the original discount from the raw order_line table ) #}
    ((order_line_item_msrp - oli.order_line_item_price) * oli.order_line_item_units) + oli.total_discount as order_line_item_total_discount,
    
    p.product_sku as order_line_item_sku,
    p.product_barcode as order_line_item_barcode,
    oli.is_gift_card as order_line_item_is_gift_card,
    oli.is_taxable as order_line_item_is_taxable

    {# //, sum(iff(ol.vendor <> 'Route', ol.total_discount,0)) as total_line_item_discount_ex_route
    //, sum(ol.total_discount) as total_line_item_discount #}

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
{# where o.order_id in ( #}
    --'4939791270087'
    {# '4939639357639' #}
    {# '3073783431367' #}
    {# '3196069478599'
    ) #}
{# order by order_id, order_line_item_id #}