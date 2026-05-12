{% test assert_column_is_not_future_date(model, column_name, grace_days=1) %}
-- Fails if any date is more than grace_days in the future (catches ETL bugs)

select *
from {{ model }}
where {{ column_name }} > current_date + interval '{{ grace_days }} day'

{% endtest %}
