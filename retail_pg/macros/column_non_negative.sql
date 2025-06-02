-- dbt test custom kiểm tra cột quantity >= 0
{% test column_non_negative(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} < 0
{% endtest %}
