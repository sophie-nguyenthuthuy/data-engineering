-- Revenue cohort retention analysis
-- Grain: one row per (acquisition_cohort × months_since_first_order)
-- Usage: dbt compile --select analyses/revenue_cohort_analysis.sql

with cohorts as (
    select
        customer_id,
        acquisition_cohort,
        first_order_date
    from {{ ref('fct_customer_ltv') }}
),

monthly_orders as (
    select
        o.customer_id,
        date_trunc('month', o.order_date)::date     as order_month,
        sum(o.net_revenue_usd)                      as monthly_revenue
    from {{ ref('fct_orders') }} o
    where not o.is_cancelled
    group by 1, 2
),

cohort_activity as (
    select
        c.acquisition_cohort,
        mo.order_month,
        extract(year from age(mo.order_month, c.acquisition_cohort))::int * 12
            + extract(month from age(mo.order_month, c.acquisition_cohort))::int
                                                    as months_since_acquisition,
        count(distinct mo.customer_id)              as active_customers,
        sum(mo.monthly_revenue)                     as cohort_revenue
    from cohorts c
    inner join monthly_orders mo using (customer_id)
    group by 1, 2, 3
),

cohort_sizes as (
    select
        acquisition_cohort,
        count(distinct customer_id) as cohort_size
    from cohorts
    group by 1
),

final as (
    select
        ca.acquisition_cohort,
        ca.order_month,
        ca.months_since_acquisition,
        cs.cohort_size,
        ca.active_customers,
        ca.cohort_revenue,

        -- Retention rate
        round(ca.active_customers::float / cs.cohort_size * 100, 2)
                                                    as retention_pct,

        -- Revenue per active customer this month
        round(ca.cohort_revenue / nullif(ca.active_customers, 0), 2)
                                                    as revenue_per_active_customer,

        -- Cumulative revenue per original cohort customer
        sum(ca.cohort_revenue) over (
            partition by ca.acquisition_cohort
            order by ca.months_since_acquisition
            rows between unbounded preceding and current row
        ) / cs.cohort_size                          as cumulative_ltv_per_customer
    from cohort_activity ca
    join cohort_sizes cs using (acquisition_cohort)
)

select * from final
order by acquisition_cohort, months_since_acquisition
