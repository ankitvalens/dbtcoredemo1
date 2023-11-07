
{% snapshot fnd_carriers %}

{{
    config(
      target_database='awsguzzle',
      target_schema='purview_dbt',
      unique_key='code',
      strategy='check',
      check_cols='all'
    )
}}

select * from {{ ref('brz_carriers') }}

{% endsnapshot %}