{{
  config(
    materialized = 'incremental',
    unique_key = ['iata','airport'],
    incremental_strategy = 'insert_overwrite'
    )
}}

SELECT
  f.*,
  origin.airport_id origin_airport_id,
  origin.airport origin_airport_name,
  origin.city origin_airport_city,
  origin.state origin_airports_state,
  dest.airport_id dest_airport_id,
  dest.airport dest_airport_name,
  dest.city dest_airport_city,
  dest.state dest_airports_state,
  c.Description carrier_description,
  CURRENT_TIMESTAMP AS last_updated_ts
FROM
  {{ ref('brz_flights') }} f
  join {{ ref('fnd_airports') }} origin on f.origin = origin.iata
  join {{ ref('fnd_airports') }} dest on f.dest = dest.iata
  join {{ ref('fnd_carriers') }} c on f.UniqueCarrier = c.code