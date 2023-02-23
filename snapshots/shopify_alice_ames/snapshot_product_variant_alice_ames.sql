{% snapshot snapshot_product_variant_alice_ames %}

{{
    config(
      target_database='aestuary_dw',
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('shopify_alice_ames', 'product_variant') }}

{% endsnapshot %}