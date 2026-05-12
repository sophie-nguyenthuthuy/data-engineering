{% macro cents_to_dollars(column_name, precision=2) %}
    round({{ column_name }} / 100.0, {{ precision }})
{% endmacro %}


{% macro dollars_to_cents(column_name) %}
    round({{ column_name }} * 100)::bigint
{% endmacro %}


{% macro safe_divide(numerator, denominator, default=0) %}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null
        then {{ default }}
        else {{ numerator }}::float / {{ denominator }}
    end
{% endmacro %}


{% macro date_diff_business_days(start_date, end_date) %}
    -- Approximate business days: total days × (5/7), rounded
    round(
        extract(day from ({{ end_date }}::timestamp - {{ start_date }}::timestamp))
        * (5.0 / 7.0)
    )::int
{% endmacro %}


{% macro current_timestamp_utc() %}
    {{ adapter.dispatch('current_timestamp_utc', 'dbt_semantic_bi_gateway')() }}
{% endmacro %}

{% macro default__current_timestamp_utc() %}
    timezone('utc', current_timestamp)
{% endmacro %}

{% macro bigquery__current_timestamp_utc() %}
    current_timestamp()
{% endmacro %}

{% macro snowflake__current_timestamp_utc() %}
    convert_timezone('UTC', current_timestamp())::timestamp_ntz
{% endmacro %}
