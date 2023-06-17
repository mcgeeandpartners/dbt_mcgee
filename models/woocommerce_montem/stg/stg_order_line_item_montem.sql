{{ config(alias="stg_order_line_item", materialized="table") }}

select ID,
    TOTAL,
    SKU,
    VARIATION_ID,
    TAX_CLASS,
    NAME,
    QUANTITY,
    SUBTOTAL,
    PRICE,
    SUBTOTAL_TAX,
    TOTAL_TAX,
    ORDER_ID,
    PRODUCT_ID,
    _FIVETRAN_SYNCED
FROM {{ source('woocommerce_montem', 'order_line_item') }} 
where not _FIVETRAN_DELETED