import pytest

from lakehouse_platform.mcp_server.resilience import CircuitBreaker, CircuitOpenError, RateLimiter


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def test_breaker_opens_after_threshold_and_fails_fast():
    clock = FakeClock()
    b = CircuitBreaker(threshold=3, cooldown=10, clock=clock)
    for _ in range(3):
        b.before_call()
        b.record_failure()
    assert b.state == "open"
    with pytest.raises(CircuitOpenError):
        b.before_call()


def test_breaker_half_opens_after_cooldown_and_closes_on_success():
    clock = FakeClock()
    b = CircuitBreaker(threshold=1, cooldown=10, clock=clock)
    b.record_failure()
    clock.t = 10
    assert b.state == "half_open"
    b.before_call()  # trial call allowed
    b.record_success()
    assert b.state == "closed"


def test_failed_half_open_trial_reopens():
    clock = FakeClock()
    b = CircuitBreaker(threshold=1, cooldown=10, clock=clock)
    b.record_failure()
    clock.t = 10
    b.before_call()
    b.record_failure()
    assert b.state == "open"


def test_success_resets_failure_count():
    b = CircuitBreaker(threshold=3, clock=FakeClock())
    b.record_failure()
    b.record_failure()
    b.record_success()
    b.record_failure()
    assert b.state == "closed"


def test_rate_limiter_enforces_burst_then_refills():
    clock = FakeClock()
    rl = RateLimiter(rate_per_min=60, burst=3, clock=clock)
    assert [rl.allow("alice") for _ in range(4)] == [True, True, True, False]
    clock.t = 1.0  # 60/min = 1 token per second
    assert rl.allow("alice")


def test_rate_limiter_isolates_colleagues():
    rl = RateLimiter(rate_per_min=60, burst=1, clock=FakeClock())
    assert rl.allow("alice")
    assert not rl.allow("alice")
    assert rl.allow("bob")
