"""GitHub notifications source for dlt.

Pulls the authenticated user's notifications (issues, PRs, mentions, review
requests, CI failures) and, for each thread, fetches the latest subject body
so downstream AI tasks have enough text to work with.

Incremental: uses GitHub's `since` parameter keyed on `updated_at`. dlt handles
state persistence across runs.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import dlt
import httpx
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

from streamdigest.config import settings

GITHUB_API = "https://api.github.com"


def _client(token: str) -> RESTClient:
    return RESTClient(
        base_url=GITHUB_API,
        auth=BearerTokenAuth(token),
        paginator=HeaderLinkPaginator(),
        headers={"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"},
    )


def _fetch_subject_body(client: httpx.Client, subject_url: str | None) -> str:
    """Fetch the body of the issue / PR / release the notification points at."""
    if not subject_url:
        return ""
    try:
        resp = client.get(subject_url, timeout=10.0)
        if resp.status_code != 200:
            return ""
        data = resp.json()
        return data.get("body") or ""
    except (httpx.HTTPError, ValueError):
        return ""


@dlt.source(name="github")
def github_source(
    token: str = dlt.secrets.value,
    include_bodies: bool = True,
):
    """dlt source emitting a `notifications` resource."""
    if not token:
        token = settings.github_token
    if not token:
        raise RuntimeError("GITHUB_TOKEN is required — set it in .env or dlt secrets.")

    @dlt.resource(
        name="notifications",
        primary_key="id",
        write_disposition="merge",
    )
    def notifications(
        updated_at: dlt.sources.incremental[str] = dlt.sources.incremental(
            "updated_at", initial_value="2024-01-01T00:00:00Z"
        ),
    ) -> Iterator[dict[str, Any]]:
        client = _client(token)
        body_client = httpx.Client(
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
            }
        )

        params = {
            "all": "true",
            "participating": "false",
            "since": updated_at.last_value,
            "per_page": 50,
        }

        try:
            for page in client.paginate("/notifications", params=params):
                for note in page:
                    subject = note.get("subject") or {}
                    row = {
                        "id": note["id"],
                        "unread": note.get("unread"),
                        "reason": note.get("reason"),
                        "updated_at": note.get("updated_at"),
                        "last_read_at": note.get("last_read_at"),
                        "repo_full_name": (note.get("repository") or {}).get("full_name"),
                        "repo_private": (note.get("repository") or {}).get("private"),
                        "subject_title": subject.get("title"),
                        "subject_type": subject.get("type"),
                        "subject_url": subject.get("url"),
                        "subject_latest_comment_url": subject.get("latest_comment_url"),
                        "body": _fetch_subject_body(body_client, subject.get("url"))
                        if include_bodies
                        else "",
                    }
                    yield row
        finally:
            body_client.close()

    return notifications
