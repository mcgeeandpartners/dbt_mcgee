{{ config(alias="fact_order_line_item") }}

select
    d.date_key, --join with dim_date
    cu.customer_key, --join with dim_customer    
    oli.order_line_item_id,
    oli.order_id, 
    oli.purchase_item_type as order_line_item_purchase_item_type, --is subscription or not
    oli.quantity as order_line_item_quantity,
    oli.total_price as order_line_item_total_price,
    oli.unit_price as order_line_item_unit_price,
    oli.tax_due as order_line_item_has_tax_due,
    oli.taxable_amount as order_line_item_taxable_amount,
    oli.is_taxable as order_line_item_is_taxable,
    oli.is_tax_included_in_unit_price as order_line_item_has_tax_included_in_unit_price,

    o.shopify_order_number,
    o.order_status, 
    o.is_prepaid as order_is_prepaid,
    o.order_total_price,
    o.order_type,
    
    o.order_placed_at_utc,
    o.order_updated_at_utc,
    o.is_deleted as order_is_deleted,

    {{ dbt_utils.generate_surrogate_key(['o.charge_id']) }} as charge_key,
    ch.subtotal_price as charge_subtotal_price,
    ch.tax_lines as charge_tax_line,
    ch.total_discounts as charge_total_discount,
    ch.total_line_items_price as charge_total_line_items_price,
    ch.total_tax as charge_total_tax,
    ch.total_price as charge_total_price,
    ch.total_refunds as charge_total_refund,
    ch.total_weight_grams as charge_total_weight_gram,
    ch.orders_count as charge_orders_count,
    ch.charge_attempts as charge_attempt_count


from {{ref('stg_order_line_item_recharge_lpc')}} as oli
inner join {{ref('stg_order_recharge_lpc')}} as o on oli.order_id = o.order_id
left join {{ref('dim_date_recharge_lpc')}} as d on o.order_placed_at_utc::date = d.date
left join {{ref('dim_customer_recharge_lpc')}} as cu on o.customer_id = cu.customer_id
left join {{ ref('stg_charge_recharge_lpc') }} as ch on o.charge_id = ch.charge_id