{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'activity_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by = ['repo_id']
  )
}}

with base as (
  select * from {{ ref('stg_events') }}
  {% if is_incremental() %}
    where activity_date >= date('{{ var("start_date") }}')
      and activity_date <= date('{{ var("end_date") }}')
  {% endif %}
)

select
  activity_date,
  repo_id,
  any_value(repo_name)                              as repo_name,
  count(*)                                          as total_events,
  countif(event_type = 'PushEvent')                 as push_events,
  countif(event_type = 'PullRequestEvent')          as pr_events,
  countif(event_type = 'IssuesEvent')               as issue_events,
  countif(event_type = 'WatchEvent')                as stars,
  countif(event_type = 'ForkEvent')                 as forks,
  count(distinct actor_id)                          as unique_actors
from base
group by activity_date, repo_id
