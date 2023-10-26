
{% snapshot fnd_planes %}

{{
    config(
      target_database='hive_metastore',
      target_schema='dbt_airlinedemodb',
      unique_key='tailnum',

      strategy='timestamp',
      updated_at='issue_date',
    )
}}

select * from {{ ref('brz_planes') }}

{% endsnapshot %}