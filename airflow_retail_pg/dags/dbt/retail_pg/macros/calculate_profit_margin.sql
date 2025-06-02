{% macro calculate_profit_margin(unit_price, unit_cost) %}
    ({{ unit_price }} - {{ unit_cost }}) / NULLIF({{ unit_cost }}, 0)
{% endmacro %}
