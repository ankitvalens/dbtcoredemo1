{{
  config(
    materialized = 'incremental',
    unique_key = ['iata','airport'],
    incremental_strategy = 'insert_overwrite'
    )
}}


select f.*,
  nvl(origin.country,'x') <> nvl(dest.country,'x') is_international ,
  DayOfWeek in (6,7) weekend,
  {% for item in ["30", "60", "120", "150"] %}
    ArrDelay > {{ item }} as arr_delay_{{ item }}min{% if not loop.last %},{% endif %}
  {% endfor %}
  ,current_timestamp refresh_ts
from
  {{ ref('fnd_flights') }} f
  left join {{ ref('fnd_airports') }} origin on f.origin_airport_id = origin.airport_id
  left join {{ ref('fnd_airports') }} dest on f.dest_airport_id = dest.airport_id