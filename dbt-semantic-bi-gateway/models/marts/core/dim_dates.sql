{{ config(materialized='table') }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2018-01-01' as date)",
        end_date="cast(current_date + interval '2 year' as date)"
    ) }}
),

final as (
    select
        cast(date_day as date)                              as date_day,

        -- ── Basic calendar fields ────────────────────────────────────────────
        extract(year  from date_day)::int                   as year,
        extract(month from date_day)::int                   as month,
        extract(day   from date_day)::int                   as day_of_month,
        extract(dow   from date_day)::int                   as day_of_week,       -- 0=Sun
        extract(doy   from date_day)::int                   as day_of_year,
        extract(week  from date_day)::int                   as iso_week_number,
        extract(quarter from date_day)::int                 as quarter,

        -- ── Human-readable labels ────────────────────────────────────────────
        to_char(date_day, 'YYYY-MM')                        as year_month,
        to_char(date_day, 'Month')                          as month_name,
        to_char(date_day, 'Mon')                            as month_name_short,
        to_char(date_day, 'Day')                            as day_name,
        to_char(date_day, 'Dy')                             as day_name_short,
        to_char(date_day, 'YYYY-"Q"Q')                      as year_quarter,
        to_char(date_day, 'IYYY-"W"IW')                    as year_iso_week,

        -- ── Week/month/quarter/year start dates ──────────────────────────────
        date_trunc('week',    date_day)::date               as week_start_date,
        date_trunc('month',   date_day)::date               as month_start_date,
        date_trunc('quarter', date_day)::date               as quarter_start_date,
        date_trunc('year',    date_day)::date               as year_start_date,

        -- ── Fiscal calendar (configurable start month via dbt var) ───────────
        case
            when extract(month from date_day)
                >= {{ var('fiscal_year_start_month', 1) }}
            then extract(year from date_day)::int
            else extract(year from date_day)::int - 1
        end                                                 as fiscal_year,

        -- ── Booleans ─────────────────────────────────────────────────────────
        extract(dow from date_day) in (0, 6)                as is_weekend,
        not (extract(dow from date_day) in (0, 6))          as is_weekday,
        date_day = date_trunc('month', date_day)::date      as is_first_day_of_month,
        date_day = (date_trunc('month', date_day)
                    + interval '1 month - 1 day')::date     as is_last_day_of_month,
        date_day <= current_date                            as is_historical,
        date_day = current_date                             as is_today
    from date_spine
)

select * from final
