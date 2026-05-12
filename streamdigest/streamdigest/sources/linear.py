"""Linear source — GraphQL notifications API, incremental on updatedAt cursor."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import dlt
import httpx

from streamdigest.config import settings

LINEAR_GRAPHQL = "https://api.linear.app/graphql"

_NOTIFICATIONS_QUERY = """
query Notifications($after: String, $updatedAfter: DateTime) {
  notifications(
    first: 50
    after: $after
    filter: { updatedAt: { gt: $updatedAfter } }
    orderBy: updatedAt
  ) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      type
      updatedAt
      readAt
      ... on IssueNotification {
        issue {
          id
          title
          description
          url
          team { key }
        }
      }
      ... on ProjectNotification {
        project {
          id
          name
          description
          url
        }
      }
    }
  }
}
"""


def _graphql(api_key: str, query: str, variables: dict) -> dict:
    resp = httpx.post(
        LINEAR_GRAPHQL,
        json={"query": query, "variables": variables},
        headers={"Authorization": api_key, "Content-Type": "application/json"},
        timeout=20.0,
    )
    resp.raise_for_status()
    payload = resp.json()
    if "errors" in payload:
        raise RuntimeError(f"Linear GraphQL error: {payload['errors']}")
    return payload["data"]


@dlt.source(name="linear")
def linear_source(api_key: str = dlt.secrets.value):
    """dlt source emitting a `notifications` resource from Linear."""
    _key = api_key or settings.linear_api_key
    if not _key:
        raise RuntimeError("LINEAR_API_KEY is required — set it in .env or dlt secrets.")

    @dlt.resource(
        name="notifications",
        primary_key="id",
        write_disposition="merge",
    )
    def notifications(
        updated_at: dlt.sources.incremental[str] = dlt.sources.incremental(
            "updated_at", initial_value="2024-01-01T00:00:00.000Z"
        ),
    ) -> Iterator[dict[str, Any]]:
        cursor: str | None = None
        since = updated_at.last_value or "2024-01-01T00:00:00.000Z"

        while True:
            variables: dict[str, Any] = {"updatedAfter": since}
            if cursor:
                variables["after"] = cursor

            data = _graphql(_key, _NOTIFICATIONS_QUERY, variables)
            page = data["notifications"]

            for node in page["nodes"]:
                issue = node.get("issue") or {}
                project = node.get("project") or {}
                subject = issue or project

                yield {
                    "id": node["id"],
                    "unread": node.get("readAt") is None,
                    "reason": node.get("type", "unknown"),
                    "updated_at": node.get("updatedAt"),
                    "last_read_at": node.get("readAt"),
                    "repo_full_name": (issue.get("team") or {}).get("key"),
                    "repo_private": None,
                    "subject_title": subject.get("title") or subject.get("name", ""),
                    "subject_type": "Issue" if issue else "Project",
                    "subject_url": subject.get("url"),
                    "subject_latest_comment_url": None,
                    "body": (subject.get("description") or "")[:8000],
                }

            if not page["pageInfo"]["hasNextPage"]:
                break
            cursor = page["pageInfo"]["endCursor"]

    return notifications
