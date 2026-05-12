with campaigns as (
    select * from {{ ref('stg_campaigns') }}
),

attributed_orders as (
    select
        campaign_id,
        count(distinct order_id)        as attributed_orders,
        count(distinct customer_id)     as attributed_customers,
        sum(net_revenue_usd)            as attributed_revenue_usd,
        sum(discount_amount_usd)        as attributed_discounts_usd
    from {{ ref('fct_orders') }}
    where campaign_id is not null
        and not is_cancelled
    group by campaign_id
),

final as (
    select
        c.campaign_id,
        c.campaign_name,
        c.channel,
        c.start_date,
        c.end_date,
        c.campaign_duration_days,
        c.budget_usd,
        c.spend_usd,
        c.budget_utilisation_pct,

        -- Attribution
        coalesce(ao.attributed_orders, 0)           as attributed_orders,
        coalesce(ao.attributed_customers, 0)        as attributed_customers,
        coalesce(ao.attributed_revenue_usd, 0)      as attributed_revenue_usd,
        coalesce(ao.attributed_discounts_usd, 0)    as attributed_discounts_usd,

        -- Derived KPIs
        case
            when c.spend_usd > 0
            then round(ao.attributed_revenue_usd / c.spend_usd, 2)
        end                                         as roas,

        case
            when nullif(ao.attributed_customers, 0) is not null
            then round(c.spend_usd / ao.attributed_customers, 2)
        end                                         as cost_per_acquisition_usd,

        case
            when nullif(ao.attributed_orders, 0) is not null
            then round(c.spend_usd / ao.attributed_orders, 2)
        end                                         as cost_per_order_usd,

        -- Profit contribution (revenue - spend)
        coalesce(ao.attributed_revenue_usd, 0)
            - c.spend_usd                           as campaign_profit_usd
    from campaigns c
    left join attributed_orders ao using (campaign_id)
)

select * from final
