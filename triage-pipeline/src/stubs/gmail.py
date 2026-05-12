"""Gmail source stub — yields synthetic emails for each tenant.

Replace with googleapiclient.discovery.build('gmail','v1') in prod. Interface
(iter_new_messages) is kept narrow so the swap is a ~20 line change.
"""
from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Iterator

from faker import Faker

_fake = Faker()
Faker.seed(42)
random.seed(42)


TEMPLATES = {
    "billing": [
        "Invoice {n} past due, please remit {amt} by {date}.",
        "Your subscription renews next week — card ending 4242 will be charged.",
        "Refund request for order #{n} — the product arrived damaged.",
    ],
    "support": [
        "Dashboard won't load after today's deploy, getting 500 on /reports.",
        "How do I export my data as CSV? Can't find the button.",
        "Integration with Slack broke — messages stopped arriving around 2pm.",
    ],
    "sales": [
        "Interested in the enterprise tier — can we schedule a demo next Tuesday?",
        "Following up on the proposal — any updates from legal review?",
        "What's the pricing for 500 seats? We'd like an annual contract.",
    ],
    "urgent": [
        "Production is down — customers reporting 502s across all regions.",
        "Security incident: suspicious login from unknown IP, need help NOW.",
    ],
    "legal": [
        "DPA review request for our vendor onboarding — deadline EOW.",
        "Subpoena received regarding user data, forwarding for counsel review.",
    ],
    "hr": [
        "Resignation notice — last day will be {date}.",
        "Benefits enrollment deadline is approaching, please confirm selections.",
    ],
    "spam": [
        "CONGRATULATIONS you won $5000 click here to claim your prize!!!",
        "Cheap meds, no prescription, fast shipping, order now.",
    ],
}


@dataclass
class RawEmail:
    id: str
    tenant_id: str
    sender: str
    subject: str
    body: str
    received_at: str
    true_label: str  # for eval only; real Gmail won't provide this

    def to_dict(self) -> dict:
        return asdict(self)


def _mint(tenant_id: str, label: str) -> RawEmail:
    body = random.choice(TEMPLATES[label]).format(
        n=random.randint(1000, 9999),
        amt=f"${random.randint(50, 5000)}",
        date=_fake.date_this_month().isoformat(),
    )
    subject = {
        "billing": "Invoice / billing question",
        "support": "Help with dashboard",
        "sales": "Pricing inquiry",
        "urgent": "URGENT — need help",
        "legal": "Legal review",
        "hr": "HR / employee matter",
        "spam": "You've won!!!",
    }[label]
    seed = f"{tenant_id}-{body}-{_fake.uuid4()}"
    return RawEmail(
        id=hashlib.sha1(seed.encode()).hexdigest()[:16],
        tenant_id=tenant_id,
        sender=_fake.email(),
        subject=subject,
        body=body,
        received_at=datetime.now(timezone.utc).isoformat(),
        true_label=label,
    )


def iter_new_messages(tenant_id: str, labels: list[str], count: int = 5) -> Iterator[RawEmail]:
    """Mint `count` synthetic emails for a tenant. ~10% chance of garbled body
    to exercise the retry + DLQ path."""
    for _ in range(count):
        label = random.choice(labels)
        email = _mint(tenant_id, label)
        if random.random() < 0.08:
            email.body = "\x00\x00INVALID\x00PAYLOAD"  # poison pill for DLQ demo
        yield email
