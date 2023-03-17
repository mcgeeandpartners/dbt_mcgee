{% snapshot snapshot_product_variant_ad %}

{{
    config(
      target_database='aestuary_dw',
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('shopify_ad', 'product_variant') }}

{% endsnapshot %}