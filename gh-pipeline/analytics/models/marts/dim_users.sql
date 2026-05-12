{{ config(materialized = 'table') }}

with events as (
  select * from {{ ref('stg_events') }}
)

select
  actor_id,
  any_value(actor_login)                       as actor_login,
  min(event_hour)                              as first_seen_at,
  max(event_hour)                              as last_seen_at,
  count(*)                                     as total_events,
  count(distinct repo_id)                      as repos_touched,
  countif(event_type = 'PushEvent')            as push_events,
  countif(event_type = 'PullRequestEvent')     as pr_events,
  countif(event_type = 'IssuesEvent')          as issue_events
from events
where actor_id is not null
group by actor_id
