{{ config(materialized = 'table') }}

with events as (
  select * from {{ ref('stg_events') }}
)

select
  repo_id,
  any_value(repo_name)                     as repo_name,
  any_value(org_login)                     as org_login,
  min(event_hour)                          as first_seen_at,
  max(event_hour)                          as last_seen_at,
  count(*)                                 as total_events,
  count(distinct actor_id)                 as unique_contributors,
  countif(event_type = 'WatchEvent')       as stars_in_window,
  countif(event_type = 'ForkEvent')        as forks_in_window
from events
where repo_id is not null
group by repo_id
