{{ config(alias="dim_refund") }}

SELECT ID,
    {{ dbt_utils.surrogate_key(['ID']) }} as refund_key,
    DATE_CREATED,
    DATE_CREATED_GMT,
    AMOUNT,
    REASON,
    REFUNDED_BY,
    REFUNDED_PAYMENT,
    API_REFUND,
    ORDER_ID
FROM {{ ref('stg_refund_montem') }}
where not _FIVETRAN_DELETED