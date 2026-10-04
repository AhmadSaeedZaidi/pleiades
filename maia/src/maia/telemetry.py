"""Bounded, process-local observations of actual scheduler cycles."""

import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

_COUNTERS = {
    "videos_updated",
    "videos_unavailable",
    "observed",
    "unavailable",
    "vault_flushed",
    "vault_failed",
    "stats_archived",
    "videos_archived",
    "videos_failed",
}
_FAILURES = {"vault_failed", "videos_failed", "searches_failed", "failed"}
_ERRORS = {"vault_flush_error", "stats_error", "key_pool_error", "search_queue_cull_error"}


@dataclass
class _Cycle:
    interval: float
    registered: float
    started: float | None = None
    finished: float | None = None
    last_success: float | None = None
    duration: float = 0.0
    failures: int = 0
    error: str | None = None
    interrupted: bool = False
    history: deque[tuple[float, dict[str, int]]] = field(default_factory=lambda: deque(maxlen=60))


class CycleMonitor:
    """Observe work without adding database writes, retries or scheduling."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self.clock = clock
        self.created = clock()
        self.cycles: dict[str, _Cycle] = {}

    def register(self, name: str, interval: float) -> None:
        if interval <= 0:
            raise ValueError("Cycle interval must be positive")
        self.cycles.setdefault(name, _Cycle(interval, self.clock()))

    def start(self, name: str) -> None:
        if name in self.cycles:
            self.cycles[name].started = self.clock()
            self.cycles[name].interrupted = False

    def finish(self, name: str, result: Any = None, *, error: Exception | None = None) -> None:
        if name not in self.cycles:
            return
        cycle = self.cycles[name]
        now = self.clock()
        cycle.duration = now - cycle.started if cycle.started is not None else 0.0
        cycle.started, cycle.finished = None, now
        data = result if isinstance(result, dict) else {}
        counters = {
            key: value
            for key, value in data.items()
            if key in _COUNTERS and type(value) is int and 0 <= value <= 2**63 - 1
        }
        cycle.history.append((now, counters))
        partial = any(data.get(key) for key in _ERRORS) or any(
            type(data.get(key)) is int and data[key] > 0 for key in _FAILURES
        )
        cycle.error = (
            type(error).__name__
            if error is not None
            else "DiscordDeliveryFailed"
            if data.get("notified") is False
            else "PartialFailure"
            if partial
            else None
        )
        if cycle.error:
            cycle.failures += 1
        else:
            cycle.failures = 0
            cycle.last_success = now

    def cancel(self, name: str) -> None:
        if name in self.cycles:
            self.cycles[name].started = None
            self.cycles[name].interrupted = True

    def snapshot(self) -> dict[str, Any] | None:
        if not self.cycles:
            return None
        now = self.clock()
        rows = []
        for name, cycle in self.cycles.items():
            anchor = cycle.finished if cycle.finished is not None else cycle.registered
            elapsed = now - cycle.started if cycle.started is not None else now - anchor
            late = elapsed > cycle.interval + max(60, cycle.interval * 0.5)
            if cycle.started is not None:
                state = "slow" if elapsed > max(900, cycle.interval * 2) else "running"
            elif cycle.interrupted:
                state = "interrupted"
            elif cycle.failures:
                state = "failed"
            elif late:
                state = "late"
            else:
                state = "starting" if cycle.finished is None else "idle"
            while cycle.history and cycle.history[0][0] < now - 3600:
                cycle.history.popleft()
            totals: dict[str, int] = {}
            for _, counters in cycle.history:
                for key, value in counters.items():
                    totals[key] = totals.get(key, 0) + value
            rows.append(
                {
                    "name": name,
                    "state": state,
                    "elapsed_seconds": elapsed,
                    "last_success_age_seconds": None
                    if cycle.last_success is None
                    else now - cycle.last_success,
                    "duration_seconds": cycle.duration,
                    "failures": cycle.failures,
                    "error": cycle.error,
                    "progress_1h": totals,
                }
            )
        return {"uptime_seconds": now - self.created, "cycles": rows}


cycle_monitor = CycleMonitor()
