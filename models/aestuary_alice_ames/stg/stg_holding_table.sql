
{{ config(
    database="AESTUARY_RAW",
    schema="SHOPIFY"
) }}


select
o.ID as ORDER_ID,
ol.ID as ORDER_LINE_ITEM_ID,
o.CUSTOMER_ID as CUSTOMER_ID,
o.TOTAL_WEIGHT as TOTAL_WEIGHT,
ol.GRAMS AS LINE_ITEM_WEIGHT,
ol.PRICE as ORDER_LINE_ITEM_PRICE,
o.TAXES_INCLUDED as TAXES_INCLUDED,
NULLIF(TRIM(o.CURRENCY),'') as CURRENCY,
o.ORDER_NUMBER as ORDER_NUMBER,
ol.REQUIRES_SHIPPING as REQUIRES_SHIPPING,
ol.PRODUCT_EXISTS as PRODUCT_EXISTS,
ol.GIFT_CARD as IS_GIFT_CARD_USED,
NULLIF(TRIM(ol.SKU),'') as PRODUCT_SKU,
ol.PRODUCT_ID as PRODUCT_ID,
ol.VARIANT_ID as PRODUCT_VARIANT_ID,
ol.TITLE as PRODUCT_TITLE,
o.CREATED_AT as ORDER_CREATED_TS,
odc.CODE as ORDER_DISCOUNT_CODE,
ol.PRE_TAX_PRICE as ORDER_LINE_ITEM_PRE_TAX_PRICE
from {{ source('ALICE_AMES_SHOPIFY', 'ORDER_LINE') }} ol left join {{ source('ALICE_AMES_SHOPIFY', 'ORDER') }} o  on o.ID = ol.ORDER_ID
left join {{ source('ALICE_AMES_SHOPIFY', 'ORDER_DISCOUNT_CODE') }} odc on o.ID = odc.ORDER_ID