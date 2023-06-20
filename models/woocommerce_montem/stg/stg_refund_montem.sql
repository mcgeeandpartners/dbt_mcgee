{{ config(alias="stg_refund") }}

select ID,
    DATE_CREATED,
    DATE_CREATED_GMT,
    AMOUNT,
    REASON,
    REFUNDED_BY,
    REFUNDED_PAYMENT,
    API_REFUND,
    ORDER_ID,
    _FIVETRAN_DELETED,
    _FIVETRAN_SYNCED
from {{ source('woocommerce_montem', 'refund') }} 