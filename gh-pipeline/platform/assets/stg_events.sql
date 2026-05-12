/* @bruin
name: {{ staging }}.stg_events
type: bq.sql
materialization:
  type: view
depends:
  - ingest_gh_archive
@bruin */

select
  id                   as event_id,
  type                 as event_type,
  event_hour,
  date(event_hour)     as activity_date,
  actor.id             as actor_id,
  actor.login          as actor_login,
  repo.id              as repo_id,
  repo.name            as repo_name,
  org.login            as org_login,
  payload.action       as action,
  created_at
from `{{ project }}.{{ raw }}.events`
where event_hour >= timestamp('{{ start_date }}')
  and event_hour <  timestamp_add(timestamp('{{ end_date }}'), interval 1 day)
