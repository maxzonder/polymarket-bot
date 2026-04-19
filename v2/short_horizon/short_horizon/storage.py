from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from .models import OrderIntent


class IntentStore(Protocol):
    def persist_intent(self, intent: OrderIntent) -> None:
        ...


@dataclass
class InMemoryIntentStore:
    intents: list[OrderIntent] = field(default_factory=list)

    def persist_intent(self, intent: OrderIntent) -> None:
        self.intents.append(intent)
