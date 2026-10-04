"""Tiny on-disk state for cross-process agent signals (no Redis required).

Used to deduplicate quota-exhausted alerts and to surface a "rate limited"
status in the heartbeat without spamming Discord on every retry loop.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, cast

# State file lives in the project root (~/.env dir's parent-independent location).
_STATE_PATH = Path("/var/lib/pleiades/agent_state.json")

# Suppress repeat quota alerts for the same agent within this window (seconds).
QUOTA_ALERT_COOLDOWN_S = 6 * 3600

# A quota-exhausted mark auto-expires after this long (seconds): quota recovers on
# its own (YouTube daily, HF hourly), so without a TTL a stale mark would be
# reported by the heartbeat forever. Agents also clear their own mark on success.
QUOTA_EXHAUSTED_TTL_S = 6 * 3600


def _read() -> dict[str, Any]:
    try:
        return dict[str, Any](json.loads(_STATE_PATH.read_text()))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _write(state: dict[str, Any]) -> None:
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _STATE_PATH.write_text(json.dumps(state, indent=2))


def mark_quota_exhausted(agent_name: str) -> None:
    """Record that *agent_name* is currently quota-exhausted."""
    state = _read()
    state.setdefault("quota_exhausted", {})[agent_name] = {
        "since": time.time(),
        "last_alert": state.get("quota_exhausted", {}).get(agent_name, {}).get("last_alert"),
    }
    _write(state)


def clear_quota_exhausted(agent_name: str) -> None:
    """Clear the quota-exhausted marker for *agent_name* (called on success)."""
    state = _read()
    if agent_name in state.get("quota_exhausted", {}):
        del state["quota_exhausted"][agent_name]
        _write(state)


def quota_exhausted_agents() -> list[str]:
    """Return agents currently marked quota-exhausted (expired marks purged)."""
    state = _read()
    marks = state.get("quota_exhausted", {})
    now = time.time()
    expired = [
        a for a, e in marks.items() if now - float(e.get("since", 0)) >= QUOTA_EXHAUSTED_TTL_S
    ]
    if expired:
        for a in expired:
            del marks[a]
        _write(state)
    return list(marks.keys())


def should_send_quota_alert(agent_name: str) -> bool:
    """True if a quota alert for *agent_name* should fire now.

    Returns False within the cooldown window since the last alert, so a
    resiliency-restart retry loop only pages once. Expired marks (see
    QUOTA_EXHAUSTED_TTL_S) are purged and treated as not-active.
    """
    state = _read()
    entry = state.get("quota_exhausted", {}).get(agent_name)
    if not entry:
        return True
    if time.time() - float(entry.get("since", 0)) >= QUOTA_EXHAUSTED_TTL_S:
        # Stale — drop it so the heartbeat stops reporting a dead mark.
        del state["quota_exhausted"][agent_name]
        _write(state)
        return True
    last = entry.get("last_alert")
    if last is None:
        return True
    return (time.time() - float(last)) >= QUOTA_ALERT_COOLDOWN_S


def record_quota_alert(agent_name: str) -> None:
    """Stamp that a quota alert for *agent_name* was just sent."""
    state = _read()
    entry = state.setdefault("quota_exhausted", {}).setdefault(agent_name, {})
    entry["last_alert"] = time.time()
    entry.setdefault("since", time.time())
    _write(state)


# Rate-limit (YouTube HTTP 403 / bot-check) back-off for the fetch agents. When a
# cycle hits rate limits we stop re-claiming on every schedule tick and back off
# with an increasing delay, so we don't hammer a flagged IP (which makes the block
# worse). Exponential: BASE * FACTOR**(attempt-1), capped at MAX.
RATE_LIMIT_BACKOFF_BASE_S = 300
RATE_LIMIT_BACKOFF_FACTOR = 2
RATE_LIMIT_BACKOFF_MAX_S = 3600


def _rate_limit_entry(agent_name: str) -> dict[str, Any]:
    state = _read()
    return cast(
        dict[str, Any],
        state.setdefault("rate_limit", {}).setdefault(
            agent_name, {"attempt": 0, "cooldown_until": None}
        ),
    )


def get_rate_limit_cooldown_until(agent_name: str) -> float | None:
    """Return the epoch second until which *agent_name* should back off, or None."""
    entry = _read().get("rate_limit", {}).get(agent_name, {})
    value = entry.get("cooldown_until")
    return float(value) if value is not None else None


def is_rate_limited(agent_name: str) -> bool:
    """True if *agent_name* is currently inside a rate-limit cooldown window."""
    cd = get_rate_limit_cooldown_until(agent_name)
    return cd is not None and time.time() < float(cd)


def bump_rate_limit_cooldown(agent_name: str) -> float:
    """Record a rate-limit hit this cycle and return the new ``cooldown_until``.

    Increases the back-off attempt and schedules the next cycle to wait
    ``BASE * FACTOR**(attempt-1)`` seconds (capped), so repeated storms back off
    progressively longer instead of re-hammering the flagged IP every tick.
    """
    state = _read()
    entry = state.setdefault("rate_limit", {}).setdefault(
        agent_name, {"attempt": 0, "cooldown_until": None}
    )
    entry["attempt"] = int(entry.get("attempt", 0)) + 1
    duration = min(
        RATE_LIMIT_BACKOFF_BASE_S * (RATE_LIMIT_BACKOFF_FACTOR ** (entry["attempt"] - 1)),
        RATE_LIMIT_BACKOFF_MAX_S,
    )
    entry["cooldown_until"] = time.time() + duration
    _write(state)
    return float(entry["cooldown_until"])


def clear_rate_limit_cooldown(agent_name: str) -> None:
    """Clear the rate-limit back-off for *agent_name* (called after a clean cycle)."""
    state = _read()
    rl = state.get("rate_limit", {})
    if agent_name in rl:
        rl[agent_name] = {"attempt": 0, "cooldown_until": None}
        _write(state)


# Paid STT (Grok/Mistral) has a daily quota; once hit we fall back to
# captions-only for the rest of the day so we don't blow the budget.
def _daily_audio_cap() -> int:
    try:
        from atlas.config import get_settings

        return get_settings().SCRIBE_DAILY_AUDIO_CAP
    except Exception:  # noqa: BLE001 - settings not available (tests)
        return 30


def _today() -> str:
    return time.strftime("%Y-%m-%d")


def audio_cap_reached() -> bool:
    """True if today's audio-fallback usage has hit the daily cap."""
    cap = _daily_audio_cap()
    state = _read()
    used: int = state.get("audio_usage", {}).get(_today(), 0)
    return used >= cap


def record_audio_usage(count: int = 1) -> None:
    """Increment today's audio-fallback usage counter."""
    state = _read()
    usage = state.setdefault("audio_usage", {})
    today = _today()
    usage = {d: c for d, c in usage.items() if d >= today}
    usage[today] = usage.get(today, 0) + count
    state["audio_usage"] = usage
    _write(state)
