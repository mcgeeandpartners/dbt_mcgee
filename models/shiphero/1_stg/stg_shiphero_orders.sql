{{ config(alias="stg_orders") }}

with base as (
    select insurance,
        fulfillment_status,
        has_dry_ice,
        order_number,
        priority_flag,
        saturday_delivery,
        source,
        allow_partial,
        total_discounts,
        adult_signature_required,
        packing_note,
        flagged,
        legacy_id,
        currency,
        id,
        email,
        alcohol,
        expected_weight_in_oz,
        required_ship_date,
        total_price,
        require_signature,
        profile,
        allocation_priority,
        allow_split,
        shop_name,
        total_tax,
        gift_invoice,
        order_date,
        account_id,
        insurance_amount,
        subtotal,
        _fivetran_synced,
        line_items,
        third_party_shipper,
        order_history,
        row_number() over (partition by id order by _fivetran_synced desc) as ranking
    from {{source('shiphero', 'orders')}}
),
orders as (
    select * from base where ranking = 1
)
select orders.*,
    li.value:node.sku::varchar as product_sku
from orders,
LATERAL FLATTEN(INPUT => line_items:edges) li