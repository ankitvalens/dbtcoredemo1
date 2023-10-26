{% test is_null(model, column_name,is_test) %}

    {% set query %}
        select
          *,
          'is_null' as test_name,
          '{{model}}' as model_name
        from
          {{ source('dbt_airlinedemodb','raw_flights') }}
        where
          {{column_name}} is null
    {% endset %}

    {% set reject_query %}
          create or replace view `dbt_airlinedemodb`.`is_null` as with source as (
              {{query}}
          ) 
          select * from source
    {% endset %}

    {% set limit_query%}
      {{query}} limit 0
    {% endset%}

    {% if is_test==True %}
        {{query}}
    {% else %} 
      {{limit_query}}   
      {% do run_query(reject_query) %}
    {% endif %}
   
{% endtest %}