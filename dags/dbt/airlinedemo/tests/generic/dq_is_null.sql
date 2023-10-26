{% test dq_is_null(model, column_name, group_by_column) %}

    {% set dq_query %}
            select
                count(*) as count,{{ group_by_column|join(', ') }}
            from
                {{model}}
            where
                {{column_name}} is not null
            group by 
                {{ group_by_column|join(', ') }}
    {% endset %}

    {% set limit_query %}
        {{ dq_query }} limit 0
    {% endset %}

    {{limit_query}}
    {% set insert_dq_query %}
        create or replace view `dbt_airlinedemodb`.`dq_summary` as with source as (
            {{dq_query}}
        )select *,CURRENT_TIMESTAMP as created_at from source
    {% endset %}
    {% do run_query(insert_dq_query) %}


{% endtest %}