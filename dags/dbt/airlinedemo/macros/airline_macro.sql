{% macro sql_columns(columns, table_alias="", alias_prefix="") %}
  {% for column in columns %}
    {{ table_alias }}{{ column }} {{ alias_prefix }}{{ column }}
    {% if not loop.last %},{% else %}{% endif %}
  {% endfor %}
{% endmacro %}

{% macro is_all_null(columns) %}
  {% for column in columns %}
    dp.{{ column }} IS NULL
    {% if not loop.last %}AND{% endif %}
  {% endfor %}
{% endmacro %}

{% macro is_column_chnaged(history_columns) %}
  {% for column in history_columns %}
    NOT (dp.{{ column }} = sp.{{ column }} OR (dp.{{ column }} IS NULL AND sp.{{ column }} IS NULL))
    {% if not loop.last %}OR{% endif %}
  {% endfor %}
{% endmacro %}

{% macro do_we_update(columns) %}
  {% for column in columns %}
    CASE WHEN scd_change_type_id = 3 and scd_row_type = 2 THEN T_{{ column }} else {{ column }} END as {{ column }}
    {% if not loop.last %},{% endif %}
  {% endfor %}
{% endmacro %}

{% macro columns_eq(columns, left_alias, right_alias) %}
  {% for column in columns %}
    {{ left_alias }}{{ column }} = {{ right_alias }}{{ column }}
  {% if not loop.last %},{% endif %}
  {% endfor %}
{% endmacro %}