from typing import Any
from ..settings import settings
from ..logger import get_logger
from .base import BaseDestination

logger = get_logger(__name__)


class EmailDestination(BaseDestination):
    """
    Sends analytics results via email (SendGrid).

    params:
        to (str | list[str]): Recipient email(s)
        subject (str): Email subject
        body_template (str): HTML template; use {rows_html} for auto-generated table
        from_email (str): Override sender address
    """

    def __init__(self, params: dict[str, Any]) -> None:
        super().__init__(params)
        self._api_key = params.get("api_key", settings.sendgrid_api_key)
        self._from_email = params.get("from_email", settings.email_from)
        to = params["to"]
        self._to = [to] if isinstance(to, str) else to
        self._subject = params.get("subject", "Reverse ETL Report")
        self._body_template = params.get("body_template", "<h2>Results</h2>{rows_html}")

    @staticmethod
    def _records_to_html(records: list[dict[str, Any]]) -> str:
        if not records:
            return "<p>No records.</p>"
        headers = list(records[0].keys())
        header_row = "".join(f"<th>{h}</th>" for h in headers)
        body_rows = ""
        for rec in records:
            cells = "".join(f"<td>{rec.get(h, '')}</td>" for h in headers)
            body_rows += f"<tr>{cells}</tr>"
        return (
            "<table border='1' cellpadding='4' cellspacing='0'>"
            f"<thead><tr>{header_row}</tr></thead>"
            f"<tbody>{body_rows}</tbody></table>"
        )

    def send(self, records: list[dict[str, Any]]) -> int:
        from sendgrid import SendGridAPIClient  # lazy import — optional dependency
        from sendgrid.helpers.mail import Mail

        rows_html = self._records_to_html(records)
        html_body = self._body_template.format(rows_html=rows_html, count=len(records))

        message = Mail(
            from_email=self._from_email,
            to_emails=self._to,
            subject=self._subject,
            html_content=html_body,
        )
        client = SendGridAPIClient(self._api_key)
        response = client.send(message)
        logger.info(
            f"EmailDestination: sent to {self._to} — "
            f"status {response.status_code}, {len(records)} rows"
        )
        return len(records)
