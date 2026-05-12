with source as (
    select * from {{ source('crm_raw', 'campaigns') }}
),

attribution as (
    select * from {{ source('crm_raw', 'campaign_attribution') }}
),

renamed as (
    select
        s.id::varchar                               as campaign_id,
        trim(s.name)                                as campaign_name,
        lower(trim(s.channel))                      as channel,
        s.start_date::date                          as start_date,
        s.end_date::date                            as end_date,
        coalesce(s.budget_usd, 0)::numeric(18, 2)  as budget_usd,
        coalesce(s.spend_usd, 0)::numeric(18, 2)   as spend_usd,

        -- Duration
        s.end_date::date - s.start_date::date       as campaign_duration_days,

        -- Budget utilisation
        case
            when s.budget_usd > 0
            then round(s.spend_usd / s.budget_usd * 100, 2)
            else null
        end                                         as budget_utilisation_pct,

        s._loaded_at::timestamp                     as _loaded_at
    from source s
)

select * from renamed
