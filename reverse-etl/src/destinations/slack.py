from typing import Any
from ..settings import settings
from ..logger import get_logger
from .base import BaseDestination

logger = get_logger(__name__)


class SlackDestination(BaseDestination):
    """
    Sends each record as a Slack message or posts a summary table.

    params:
        channel (str): Slack channel ID or name, e.g. "#alerts"
        message_template (str): Python format string, e.g. "User {name} churned ({mrr} MRR)"
        batch_summary (bool): If true, sends one aggregated message instead of per-row messages
        token (str): Override bot token (falls back to SLACK_BOT_TOKEN env var)
    """

    def __init__(self, params: dict[str, Any]) -> None:
        super().__init__(params)
        from slack_sdk import WebClient  # lazy import — optional dependency
        token = params.get("token", settings.slack_bot_token)
        self._client = WebClient(token=token)
        self._channel = params["channel"]
        self._template = params.get("message_template", "{record}")
        self._batch_summary = params.get("batch_summary", False)

    def _format_record(self, record: dict[str, Any]) -> str:
        try:
            return self._template.format(**record, record=record)
        except KeyError as e:
            return f"[template error: missing key {e}] {record}"

    def _post(self, text: str) -> None:
        self._client.chat_postMessage(channel=self._channel, text=text)

    def send(self, records: list[dict[str, Any]]) -> int:
        from slack_sdk.errors import SlackApiError  # lazy import
        if not records:
            return 0

        if self._batch_summary:
            lines = [f"*Reverse ETL sync — {len(records)} record(s)*"]
            for r in records:
                lines.append(f"• {self._format_record(r)}")
            try:
                self._post("\n".join(lines))
                logger.info(f"SlackDestination: sent batch summary to {self._channel}")
                return len(records)
            except SlackApiError as e:
                logger.error(f"SlackDestination error: {e.response['error']}")
                raise

        synced = 0
        for record in records:
            try:
                self._post(self._format_record(record))
                synced += 1
            except SlackApiError as e:
                logger.error(f"SlackDestination: failed on record {record}: {e.response['error']}")
        logger.info(f"SlackDestination: sent {synced}/{len(records)} messages to {self._channel}")
        return synced
