
{% snapshot fnd_carriers %}

{{
    config(
      target_database='hive_metastore',
      target_schema='dbt_airlinedemodb',
      unique_key='code',
      strategy='check',
      check_cols='all'
    )
}}

select * from {{ ref('brz_carriers') }}

{% endsnapshot %}