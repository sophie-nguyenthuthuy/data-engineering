with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

campaigns as (
    select * from {{ source('crm_raw', 'campaign_attribution') }}
),

campaign_details as (
    select * from {{ ref('stg_campaigns') }}
),

orders_with_customers as (
    select
        o.*,
        c.email,
        c.full_name                         as customer_name,
        c.country_code,
        c.acquisition_channel,
        c.first_seen_at                     as customer_created_at,
        c.is_email_verified,
        c.is_marketing_subscribed
    from orders o
    left join customers c using (customer_id)
),

with_attribution as (
    select
        owc.*,
        ca.campaign_id,
        cd.campaign_name,
        cd.channel                          as campaign_channel
    from orders_with_customers owc
    left join campaigns ca
        on owc.order_id = ca.order_id
    left join campaign_details cd
        on ca.campaign_id = cd.campaign_id
)

select * from with_attribution
