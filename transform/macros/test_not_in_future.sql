{# Generic test: fails for any row whose date is after today. #}
{% test not_in_future(model, column_name) %}
    select * from {{ model }} where {{ column_name }} > current_date
{% endtest %}
