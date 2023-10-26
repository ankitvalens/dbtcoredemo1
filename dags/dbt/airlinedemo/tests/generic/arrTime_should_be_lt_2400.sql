{% test arrTime_should_be_lt_2400(model, column_name, is_test) %}

    {% set query %}
        select
          *,
          'arrTime should be less than 2400' as test_name,
          '{{model}}' as model_name
        from
          {{ source('dbt_airlinedemodb','stg_flights') }}
        where
          ArrTime >= 2400
    {% endset %}

    {% set reject_query %}
        create or replace view `dbt_airlinedemodb`.`arrTime_2400` as with source as (
            {{query}}
        ) 
        select * from source
    {% endset %}

     {% set query_limit_0 %}
        {{query}} limit 0
     {% endset %}


    {% if is_test==True %}
        {{query}}
    {% else %}
        {{query_limit_0}}
        {% do run_query(reject_query) %}
    {% endif %}
    
   
{% endtest %}