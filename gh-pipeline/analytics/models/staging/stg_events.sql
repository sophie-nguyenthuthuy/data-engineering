{{
  config(
    materialized = 'view',
    partition_by = {'field': 'event_hour', 'data_type': 'timestamp', 'granularity': 'hour'}
  )
}}

with raw as (
  select *
  from {{ source('gh_raw', 'events') }}
  where event_hour >= timestamp('{{ var("start_date") }}')
    and event_hour <  timestamp_add(timestamp('{{ var("end_date") }}'), interval 1 day)
)

select
  id                                              as event_id,
  type                                            as event_type,
  event_hour,
  date(event_hour)                                as activity_date,
  actor.id                                        as actor_id,
  actor.login                                     as actor_login,
  repo.id                                         as repo_id,
  repo.name                                       as repo_name,
  org.login                                       as org_login,
  payload.action                                  as action,
  cast(public as bool)                            as is_public,
  created_at
from raw
