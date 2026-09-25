"""Small, dependency-free resilience primitives for the gateway.

CircuitBreaker: stop hammering a failing dependency. After `threshold` consecutive
failures it opens and fails fast for `cooldown` seconds, then lets one trial
call through (half-open). This protects Trino during an incident and gives the
agent an immediate, honest "data unavailable" instead of hanging while a customer
is on the line.

RateLimiter: token bucket per colleague, so one runaway agent loop cannot starve
everyone else. Per-replica here; use Redis or the API gateway across replicas.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field


class CircuitOpenError(RuntimeError):
    pass


@dataclass
class CircuitBreaker:
    threshold: int = 5
    cooldown: float = 30.0
    clock: callable = time.monotonic
    _failures: int = 0
    _opened_at: float | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def state(self) -> str:
        with self._lock:
            if self._opened_at is None:
                return "closed"
            return "half_open" if self.clock() - self._opened_at >= self.cooldown else "open"

    def before_call(self) -> None:
        if self.state == "open":
            raise CircuitOpenError("dependency unavailable (circuit open); failing fast")

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._opened_at = None

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._failures >= self.threshold or self._opened_at is not None:
                self._opened_at = self.clock()  # (re)open; a failed half-open trial re-arms


@dataclass
class RateLimiter:
    rate_per_min: int = 60
    burst: int = 20
    clock: callable = time.monotonic
    _buckets: dict[str, tuple[float, float]] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def allow(self, key: str) -> bool:
        now = self.clock()
        with self._lock:
            tokens, last = self._buckets.get(key, (float(self.burst), now))
            tokens = min(self.burst, tokens + (now - last) * self.rate_per_min / 60.0)
            if tokens < 1:
                self._buckets[key] = (tokens, now)
                return False
            self._buckets[key] = (tokens - 1, now)
            return True
