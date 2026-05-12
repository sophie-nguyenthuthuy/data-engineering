"""Slack source — conversations_history incremental on ts cursor per channel."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

import dlt

from streamdigest.config import settings


def _ts_to_iso(ts: str | None) -> str | None:
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
    except (ValueError, TypeError):
        return ts


def _classify_reason(msg: dict, bot_user_id: str | None) -> str:
    if msg.get("channel_type") == "im":
        return "dm"
    text = msg.get("text", "")
    if bot_user_id and f"<@{bot_user_id}>" in text:
        return "mention"
    return "channel_post"


def _fetch_replies(client: Any, channel: str, thread_ts: str) -> str:
    """Return all reply texts joined, skipping the parent message."""
    try:
        resp = client.conversations_replies(channel=channel, ts=thread_ts, limit=20)
        msgs = resp.get("messages", [])[1:]  # skip parent
        return "\n".join(m.get("text", "") for m in msgs if m.get("text"))
    except Exception:
        return ""


@dlt.source(name="slack")
def slack_source(
    bot_token: str = dlt.secrets.value,
    channel_limit: int = 50,
    message_limit: int = 200,
):
    """dlt source emitting a `messages` resource from all joined Slack channels."""
    _token = bot_token or settings.slack_bot_token
    if not _token:
        raise RuntimeError(
            "SLACK_BOT_TOKEN is required — set it in .env or dlt secrets.\n"
            "Required scopes: channels:history, groups:history, im:history, mpim:history, "
            "channels:read, groups:read, im:read, mpim:read, users:read"
        )

    @dlt.resource(
        name="messages",
        primary_key="id",
        write_disposition="merge",
    )
    def messages(
        last_ts: dlt.sources.incremental[str] = dlt.sources.incremental(
            "ts", initial_value="0"
        ),
    ) -> Iterator[dict[str, Any]]:
        try:
            from slack_sdk import WebClient  # type: ignore[import]
            from slack_sdk.errors import SlackApiError  # type: ignore[import]
        except ImportError as exc:
            raise ImportError(
                "slack-sdk is required for the Slack source. "
                "Install it with: pip install slack-sdk"
            ) from exc

        client = WebClient(token=_token)
        since_ts = last_ts.last_value or "0"

        # Resolve bot's own user ID for mention detection
        try:
            bot_user_id = client.auth_test()["user_id"]
        except Exception:
            bot_user_id = None

        # Enumerate all joined conversations
        cursor = None
        channels: list[dict] = []
        while True:
            resp = client.conversations_list(
                types="public_channel,private_channel,im,mpim",
                limit=channel_limit,
                cursor=cursor,
            )
            channels.extend(
                c for c in resp.get("channels", []) if c.get("is_member")
            )
            cursor = (resp.get("response_metadata") or {}).get("next_cursor")
            if not cursor:
                break

        for channel in channels:
            channel_id = channel["id"]
            channel_name = channel.get("name") or channel_id

            try:
                history_cursor = None
                while True:
                    resp = client.conversations_history(
                        channel=channel_id,
                        oldest=since_ts,
                        limit=message_limit,
                        cursor=history_cursor,
                    )
                    for msg in resp.get("messages", []):
                        ts = msg.get("ts", "")
                        if not ts or ts <= since_ts:
                            continue

                        text = msg.get("text", "")
                        if msg.get("reply_count", 0) > 0:
                            replies = _fetch_replies(client, channel_id, ts)
                            body = f"{text}\n{replies}".strip() if replies else text
                        else:
                            body = text

                        title = (text.split("\n")[0] or "")[:120]
                        reason = _classify_reason(msg, bot_user_id)
                        updated_at = _ts_to_iso(ts)

                        yield {
                            "id": f"{channel_id}-{ts}",
                            "ts": ts,
                            "unread": True,
                            "reason": reason,
                            "updated_at": updated_at,
                            "last_read_at": None,
                            "repo_full_name": channel_name,
                            "repo_private": channel.get("is_private"),
                            "subject_title": title,
                            "subject_type": "SlackMessage",
                            "subject_url": None,
                            "subject_latest_comment_url": None,
                            "body": body[:8000],
                            "channel_id": channel_id,
                            "user": msg.get("user"),
                        }

                    history_cursor = (resp.get("response_metadata") or {}).get("next_cursor")
                    if not history_cursor:
                        break

            except SlackApiError:
                continue

    return messages
