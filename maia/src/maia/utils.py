"""Shared utilities for Maia agents.

Centralises helpers that were previously duplicated across agent modules
(retry wrappers, common exceptions).
"""

import asyncio
import logging
import re
from collections.abc import Callable, Coroutine
from typing import Any

from atlas.vault_io import run_vault_io

logger = logging.getLogger(__name__)

# Compatibility name for existing agents. The backend owns retry policy.
vault_op_with_retry = run_vault_io

YT_RATE_LIMIT_PATTERNS = [
    re.compile(r"429", re.IGNORECASE),
    re.compile(r"too many requests", re.IGNORECASE),
    re.compile(r"rate.?limit", re.IGNORECASE),
    re.compile(r"sign in to confirm", re.IGNORECASE),
    re.compile(r"this content isn.t available.*try again later", re.IGNORECASE),
    re.compile(r"unable to extract", re.IGNORECASE),
    re.compile(r"http error 403", re.IGNORECASE),
]


def looks_like_rate_limit(stderr: str) -> bool:
    """Heuristic check — does *stderr* contain known YouTube rate-limit signals?"""
    return any(p.search(stderr) for p in YT_RATE_LIMIT_PATTERNS)


async def notify_quota_exhausted(agent_name: str) -> None:
    """Send a Discord alert that all API keys are exhausted for *agent_name*.

    Rate-limited to one per cooldown window per agent (so the resiliency
    restart/retry loop does not spam Discord); the exhausted state is also
    recorded so the heartbeat can show a "rate limited" status.
    """
    from atlas.state import (
        mark_quota_exhausted,
        record_quota_alert,
        should_send_quota_alert,
    )

    mark_quota_exhausted(agent_name)
    if not should_send_quota_alert(agent_name):
        logger.info(f"Quota-exhausted alert for {agent_name} suppressed (within cooldown).")
        return

    try:
        from atlas.notifications import AlertChannel, AlertLevel, notifier

        await notifier.send(
            title=f"⚠ Quota Exhausted: {agent_name}",
            description=(
                f"All API keys for the **{agent_name}** agent have been exhausted.\n\n"
                "**What this means:**\n"
                f"• {agent_name} is now effectively paused\n"
                "• Other agents using the same key pool are also affected\n"
                "• Manual key rotation or wait-for-reset is required\n\n"
                "This alert is rate-limited (one per 6h). The fleet heartbeat "
                "will continue to show a **rate limited** status until keys recover."
            ),
            channel=AlertChannel.ALERTS,
            level=AlertLevel.CRITICAL,
            fields={
                "Agent": agent_name,
                "Action": "All keys exhausted — agent paused",
                "Environment": "VPS (no auto-rotation)",
            },
        )
        record_quota_alert(agent_name)
    except Exception:
        logger.exception(f"Failed to send quota-exhausted notification for {agent_name}")


def run_agent_main(run_coro: Callable[[], Coroutine[Any, Any, Any]], name: str) -> None:
    """Run an agent's entry-point coroutine with uniform SIGINT/error handling.

    Replaces the near-identical ``try/except KeyboardInterrupt/except Exception``
    block repeated in every agent module's ``main()``.

    ``run_coro`` is a zero-arg callable returning the plain operation coroutine
    to execute (for example ``lambda: streamer_operation(batch_size=...)``).
    """
    try:
        asyncio.run(run_coro())
    except KeyboardInterrupt:
        logging.getLogger(name).info(f"{name} stopped by user (SIGINT)")
    except Exception as e:
        logging.getLogger(name).exception(f"{name} failed with error: {e}")
        raise


def cli_bootstrap() -> None:
    """Configure root logging for a CLI agent run (idempotent)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def video_id_of(item: dict[str, Any]) -> str | None:
    """Extract a video ID from a YouTube API ``search``/``videos`` item.

    The ``id`` field is a bare string (``videos.list``) or a dict with a
    ``videoId`` key (``search.list``). Returns ``None`` when absent.
    """
    vid = item.get("id")
    if isinstance(vid, dict):
        return vid.get("videoId")
    return vid if isinstance(vid, str) else None


def channel_ids_of(items: list[dict[str, Any]]) -> list[str]:
    """Collect the non-empty ``channelId`` values from a list of API items."""
    ids: list[str] = []
    for item in items:
        cid = item.get("snippet", {}).get("channelId")
        if cid:
            ids.append(cid)
    return ids
