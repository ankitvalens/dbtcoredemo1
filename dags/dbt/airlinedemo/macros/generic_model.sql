{% macro sri_generic(source) %}
  with
    source as (
        select * from {{ source }}
    ),
    job_instance_id as (
        select (floor(random()*1000000000000)) as job_instance_id
    ),
    sri_generic as (
        select job_instance_id, s.* from source s, job_instance_id
    )

    select * from sri_generic
{% endmacro %}