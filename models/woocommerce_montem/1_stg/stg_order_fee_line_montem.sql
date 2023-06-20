{{ config(alias="stg_order_fee_line") }}

SELECT 
    id as ofl_id, 
    total, 
    1 as quantity, 
    total as subtotal, 
    total as price, 
    order_id
FROM {{ source('woocommerce_montem', 'order_fee_line') }}