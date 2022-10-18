select 
c.CUSTOMER_KEY,
d.DATE_KEY,
p.PRODUCT_KEY,
o.CREATED_AT AS ORDER_CREATED_DATE
from {{ source('ALICE_AMES_SHOPIFY', 'ORDER') }} o left join {{ source('ALICE_AMES_SHOPIFY', 'ORDER_LINE') }} ol on o.ID = ol.ORDER_ID
left join {{ ref('dim_customer') }} c on o.CUSTOMER_ID = c.CUSTOMER_ID
left join {{ ref('dim_date') }} d on d."DATE" = DATE(o.CREATED_AT)
left join {{ ref('dim_product_variant') }} p on ol.SKU = p.PRODUCT_SKU
    -- case when ol.PRODUCT_ID is not null then TO_VARCHAR(ol.PRODUCT_ID)
    -- when ol.PRODUCT_ID is null and ol.SKU is not null then ol.SKU
    -- else lower(ol.name)
    -- end
    -- =
    -- case when ol.PRODUCT_ID is not null then TO_VARCHAR(p.PRODUCT_ID)
    -- when ol.PRODUCT_ID is null and ol.SKU is not null then p.PRODUCT_SKU
    -- else lower(p.PRODUCT_TITLE)
    -- end