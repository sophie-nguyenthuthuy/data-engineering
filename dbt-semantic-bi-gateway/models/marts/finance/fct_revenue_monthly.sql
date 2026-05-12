with daily as (
    select * from {{ ref('fct_revenue_daily') }}
),

monthly as (
    select
        date_trunc('month', date_day)::date     as month_start_date,
        to_char(date_day, 'YYYY-MM')            as year_month,
        extract(year from date_day)::int        as year,
        extract(month from date_day)::int       as month,

        sum(total_orders)                       as total_orders,
        sum(unique_customers)                   as total_unique_customers,
        sum(gross_revenue_usd)                  as gross_revenue_usd,
        sum(net_revenue_usd)                    as net_revenue_usd,
        sum(total_discounts_usd)                as total_discounts_usd,
        sum(total_cogs_usd)                     as total_cogs_usd,
        sum(total_gross_profit_usd)             as total_gross_profit_usd,
        sum(total_units_sold)                   as total_units_sold,
        sum(returned_orders)                    as returned_orders,
        avg(avg_order_value_usd)                as avg_order_value_usd,

        case
            when sum(net_revenue_usd) > 0
            then round(
                sum(total_gross_profit_usd) / sum(net_revenue_usd) * 100, 2
            )
        end                                     as gross_margin_pct
    from daily
    group by 1, 2, 3, 4
),

with_growth as (
    select
        *,
        -- MoM growth
        lag(net_revenue_usd) over (order by month_start_date)
                                                as prev_month_revenue_usd,
        case
            when lag(net_revenue_usd) over (order by month_start_date) > 0
            then round(
                (net_revenue_usd
                    - lag(net_revenue_usd) over (order by month_start_date))
                / lag(net_revenue_usd) over (order by month_start_date) * 100,
                2
            )
        end                                     as revenue_mom_growth_pct,

        -- YoY growth (same month prior year)
        lag(net_revenue_usd, 12) over (order by month_start_date)
                                                as prior_year_revenue_usd,
        case
            when lag(net_revenue_usd, 12) over (order by month_start_date) > 0
            then round(
                (net_revenue_usd
                    - lag(net_revenue_usd, 12) over (order by month_start_date))
                / lag(net_revenue_usd, 12) over (order by month_start_date) * 100,
                2
            )
        end                                     as revenue_yoy_growth_pct,

        -- Cumulative YTD
        sum(net_revenue_usd) over (
            partition by year
            order by month_start_date
            rows between unbounded preceding and current row
        )                                       as ytd_revenue_usd
    from monthly
)

select * from with_growth
