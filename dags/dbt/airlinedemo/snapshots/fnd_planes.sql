
{% snapshot fnd_planes %}

{{
    config(
      target_database='awsguzzle',
      target_schema='purview_dbt',
      unique_key='tailnum',

      strategy='timestamp',
      updated_at='issue_date',
    )
}}

select * from {{ ref('brz_planes') }}

{% endsnapshot %}