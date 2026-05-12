from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class PaymentStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    COMPENSATED = "COMPENSATED"


class TransactionStep(str, Enum):
    CREATED = "CREATED"
    KAFKA_PUBLISHED = "KAFKA_PUBLISHED"
    WAREHOUSE_WRITTEN = "WAREHOUSE_WRITTEN"
    NOTIFICATION_SENT = "NOTIFICATION_SENT"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    COMPENSATING = "COMPENSATING"


class PaymentEvent(BaseModel):
    payment_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    idempotency_key: str = Field(default_factory=lambda: str(uuid.uuid4()))
    sender_account: str
    receiver_account: str
    amount: Decimal
    currency: str = "USD"
    description: str = ""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    status: PaymentStatus = PaymentStatus.PENDING
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = {"json_encoders": {Decimal: str, datetime: lambda v: v.isoformat()}}


class OutboxEntry(BaseModel):
    id: Optional[int] = None
    idempotency_key: str
    aggregate_type: str = "payment"
    aggregate_id: str
    event_type: str = "PaymentCreated"
    payload: dict[str, Any]
    created_at: datetime = Field(default_factory=datetime.utcnow)
    published_at: Optional[datetime] = None
    retry_count: int = 0
    last_error: Optional[str] = None


class TransactionState(BaseModel):
    transaction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    idempotency_key: str
    payment_id: str
    current_step: TransactionStep = TransactionStep.CREATED
    kafka_published: bool = False
    warehouse_ack: bool = False
    notification_ack: bool = False
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def is_complete(self) -> bool:
        return self.kafka_published and self.warehouse_ack and self.notification_ack
