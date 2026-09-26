"""A daily spend cap on model calls, shared by every AI feature in the assistant.

Each successful call records its estimated cost from the response's token usage. Once
today's estimate reaches the cap, `allow()` says no and callers fall back (rules for
understanding, the template for the call note), saying why. The cap resets at midnight
UTC. It is an estimate at list price, a guard rail rather than billing; the provider's
console remains the source of truth.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any

from prometheus_client import Gauge

# USD per million tokens (list price): input, output. Cache reads bill at 0.1x input and
# 5-minute cache writes at 1.25x. Unknown models are priced high on purpose, so the cap
# errs strict until the table is updated.
PRICES = {"claude-haiku-4-5": (1.0, 5.0), "claude-sonnet-5": (2.0, 10.0)}
UNKNOWN_PRICE = (15.0, 75.0)
DEFAULT_DAILY_USD = 0.25

SPEND = Gauge("assist_llm_spend_usd_today", "Estimated model spend today (list price)")
CAP = Gauge("assist_llm_budget_usd", "Daily model spend cap")


def cost_usd(model: str, usage: Any) -> float:
    """Estimated cost of one response from its `usage` (SDK object or dict)."""

    def get(name: str) -> int:
        value = usage.get(name) if isinstance(usage, dict) else getattr(usage, name, None)
        return value or 0

    per_in, per_out = next((p for name, p in PRICES.items() if model.startswith(name)), UNKNOWN_PRICE)
    return (
        get("input_tokens") * per_in
        + get("cache_read_input_tokens") * per_in * 0.1
        + get("cache_creation_input_tokens") * per_in * 1.25
        + get("output_tokens") * per_out
    ) / 1e6


class SpendBudget:
    def __init__(
        self, daily_usd: float, today: Callable[[], date] = lambda: datetime.now(UTC).date()
    ) -> None:
        self.daily_usd = daily_usd
        self.today = today
        self._day = today()
        self._spent = 0.0
        self._lock = threading.Lock()  # extraction runs in worker threads
        CAP.set(daily_usd)

    @classmethod
    def from_env(cls) -> SpendBudget:
        return cls(float(os.environ.get("CALL_ASSIST_DAILY_BUDGET_USD", DEFAULT_DAILY_USD)))

    def _roll(self) -> None:
        if (d := self.today()) != self._day:
            self._day, self._spent = d, 0.0

    @property
    def spent(self) -> float:
        with self._lock:
            self._roll()
            return self._spent

    def allow(self) -> bool:
        return self.spent < self.daily_usd

    def record(self, model: str, usage: Any) -> float:
        cost = cost_usd(model, usage)
        with self._lock:
            self._roll()
            self._spent += cost
            SPEND.set(self._spent)
        return cost

    def reason(self) -> str:
        return f"daily AI budget reached (${self.spent:.2f} of ${self.daily_usd:.2f})"


BUDGET = SpendBudget.from_env()
