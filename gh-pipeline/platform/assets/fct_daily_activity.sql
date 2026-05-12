/* @bruin
name: {{ marts }}.fct_daily_activity
type: bq.sql
materialization:
  type: table
  strategy: merge
  partition_by: activity_date
  cluster_by: [repo_id]
  unique_key: [activity_date, repo_id]
depends:
  - {{ staging }}.stg_events
@bruin */

select
  activity_date,
  repo_id,
  any_value(repo_name)                          as repo_name,
  count(*)                                      as total_events,
  countif(event_type = 'PushEvent')             as push_events,
  countif(event_type = 'PullRequestEvent')      as pr_events,
  countif(event_type = 'IssuesEvent')           as issue_events,
  countif(event_type = 'WatchEvent')            as stars,
  countif(event_type = 'ForkEvent')             as forks,
  count(distinct actor_id)                      as unique_actors
from `{{ project }}.{{ staging }}.stg_events`
group by activity_date, repo_id
