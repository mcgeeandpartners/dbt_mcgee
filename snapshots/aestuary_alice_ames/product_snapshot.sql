{% snapshot product_snapshot %}

{{
    config(
      target_database='AESTUARY_RAW',
      target_schema='SHOPIFY',
      unique_key='ID',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('ALICE_AMES_SHOPIFY', 'PRODUCT') }}

{% endsnapshot %}