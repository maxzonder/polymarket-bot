from __future__ import annotations

from enum import StrEnum


class OrderState(StrEnum):
    INTENT = "intent"
    PENDING_SEND = "pending_send"
    ACCEPTED = "accepted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCEL_REQUESTED = "cancel_requested"
    CANCEL_CONFIRMED = "cancel_confirmed"
    REJECTED = "rejected"
    EXPIRED = "expired"
    UNKNOWN = "unknown"
    REPLACE_REQUESTED = "replace_requested"
    REPLACED = "replaced"
