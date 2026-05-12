with customers as (
    select * from {{ ref('dim_customers') }}
),

-- Simple RFM scoring (1–5 each dimension)
rfm as (
    select
        customer_id,
        days_since_last_order,
        total_orders,
        net_revenue_usd,

        -- Recency score: lower days = higher score
        ntile(5) over (order by days_since_last_order desc)  as recency_score,
        -- Frequency score
        ntile(5) over (order by total_orders asc)            as frequency_score,
        -- Monetary score
        ntile(5) over (order by net_revenue_usd asc)         as monetary_score
    from customers
    where has_purchased
),

rfm_combined as (
    select
        *,
        recency_score + frequency_score + monetary_score    as rfm_total_score,
        recency_score::text
            || frequency_score::text
            || monetary_score::text                         as rfm_segment_code
    from rfm
),

final as (
    select
        c.customer_id,
        c.email,
        c.country_code,
        c.acquisition_channel,
        c.acquisition_cohort,
        c.customer_segment,
        c.recency_segment,
        c.ltv_tier,

        -- Financial
        c.gross_revenue_usd,
        c.net_revenue_usd,
        c.total_orders,
        c.completed_orders,
        c.avg_order_value_usd,
        c.total_discounts_usd,

        -- Dates
        c.first_order_date,
        c.last_order_date,
        c.days_since_last_order,

        -- RFM
        r.recency_score,
        r.frequency_score,
        r.monetary_score,
        r.rfm_total_score,
        r.rfm_segment_code,

        -- Predicted 12-month LTV (naive: avg_order_value × predicted_orders)
        -- Replace with ML model output via dbt Python model in prod
        round(
            c.avg_order_value_usd
            * (c.total_orders::float / greatest(
                extract(month from age(c.last_order_date, c.first_order_date)),
                1
            )) * 12,
            2
        )                                       as predicted_ltv_12m_usd
    from customers c
    left join rfm_combined r using (customer_id)
    where c.has_purchased
)

select * from final
