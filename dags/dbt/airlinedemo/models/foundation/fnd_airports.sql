 {{
  config(
    materialized = 'incremental',
    unique_key = ['iata','airport'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['created_at']
    )
}}

WITH src_with_seq_id AS (
  SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['iata','airport']) }} as airport_id,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
  FROM
    {{ ref('brz_airports') }}
)

SELECT
  *
FROM
  src_with_seq_id