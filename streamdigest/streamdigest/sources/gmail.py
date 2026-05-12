"""Gmail source — IMAP via imap-tools, incremental on UID cursor."""

from __future__ import annotations

from collections.abc import Iterator
from email.utils import parsedate_to_datetime
from typing import Any

import dlt

from streamdigest.config import settings

IMAP_HOST = "imap.gmail.com"


@dlt.source(name="gmail")
def gmail_source(
    host: str = dlt.config.value,
    email: str = dlt.secrets.value,
    app_password: str = dlt.secrets.value,
    folder: str = "INBOX",
    batch_size: int = 50,
):
    """dlt source emitting a `messages` resource from a Gmail IMAP mailbox."""
    _host = host or settings.gmail_host or IMAP_HOST
    _email = email or settings.gmail_email
    _password = app_password or settings.gmail_app_password

    if not (_email and _password):
        raise RuntimeError(
            "GMAIL_EMAIL and GMAIL_APP_PASSWORD are required — set them in .env"
        )

    @dlt.resource(
        name="messages",
        primary_key="id",
        write_disposition="merge",
    )
    def messages(
        last_uid: dlt.sources.incremental[int] = dlt.sources.incremental(
            "uid", initial_value=1
        ),
    ) -> Iterator[dict[str, Any]]:
        try:
            from imap_tools import MailBox, AND  # type: ignore[import]
        except ImportError as exc:
            raise ImportError(
                "imap-tools is required for the Gmail source. "
                "Install it with: pip install imap-tools"
            ) from exc

        since_uid = last_uid.last_value or 1

        with MailBox(_host).login(_email, _password, initial_folder=folder) as mb:
            criteria = AND(uid=f"{since_uid}:*")
            for msg in mb.fetch(criteria, bulk=batch_size, mark_seen=False):
                uid = int(msg.uid)
                if uid < since_uid:
                    continue

                try:
                    updated_at = msg.date.isoformat() if msg.date else None
                except Exception:
                    updated_at = None

                from_addr = msg.from_ or ""
                reason = "direct" if _email.lower() in (msg.to or []) else "cc"

                body = msg.text or msg.html or ""
                if msg.html and not msg.text:
                    import re
                    body = re.sub(r"<[^>]+>", " ", body).strip()

                yield {
                    "id": msg.message_id or f"uid-{uid}",
                    "uid": uid,
                    "unread": not msg.seen,
                    "reason": reason,
                    "updated_at": updated_at,
                    "last_read_at": None,
                    "repo_full_name": None,
                    "repo_private": None,
                    "subject_title": msg.subject or "(no subject)",
                    "subject_type": "Email",
                    "subject_url": None,
                    "subject_latest_comment_url": None,
                    "body": body[:8000],
                    "from_address": from_addr,
                    "folder": folder,
                }

    return messages
