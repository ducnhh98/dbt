{% macro get_max_change_date(relation, column_name) %}
    {% if not execute %}
        {{ return(none) }}
    {% endif %}

    {% set sql %}
        SELECT max({{ column_name }}) AS max_value
        FROM {{ relation }}
    {% endset %}

    {% set result = run_query(sql) %}

    {% if result and result.rows | length > 0 %}
        {{ return(result.columns[0].values()[0]) }}
    {% else %}
        {{ return(none) }}
    {% endif %}
{% endmacro %}
