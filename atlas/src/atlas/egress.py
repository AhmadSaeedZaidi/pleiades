"""Shared egress-IP pool for YouTube-bound agents.

YouTube throttles the caption/stream surfaces *per source IP*, so spreading
requests across several egress IPs (a SOCKS proxy on a clean host, the executor
VPS's own egress, etc.) raises throughput — each fresh IP gets a fresh
allowance. This module is the single home of that rotation logic so the Scribe,
Streamer, Singer and Painter all share one pool instead of each re-implementing
it (DRY).

Configuration
-------------
``YOUTUBE_PROXY`` is a single URL, a comma/space-separated list of URLs, or a
list containing the literal ``direct`` (meaning "no proxy" — the executor
VPS's own egress). Example::

    YOUTUBE_PROXY="socks5h://127.0.0.1:1090,direct"

The pool hands out sessions/connections round-robin and, when an IP is
throttled, advances to the next one so a blocked IP yields to a fresh one
rather than failing the request.
"""

from __future__ import annotations

import itertools
import logging
import os
from collections.abc import Iterable, Iterator

logger = logging.getLogger("atlas.egress")

# Sentinel meaning "use the executor VPS's own egress" (no proxy).
DIRECT = "direct"


def parse_proxy_pool(raw: str | None = None) -> list[str | None]:
    """Parse ``YOUTUBE_PROXY`` into an ordered list of egress specs.

    Each entry is either a proxy URL or ``None`` (the ``direct`` sentinel).
    Returns ``[None]`` when unset so callers can always iterate.
    """
    raw = raw if raw is not None else os.environ.get("YOUTUBE_PROXY")
    if not raw:
        return [None]
    out: list[str | None] = []
    for part in raw.replace(",", " ").split():
        part = part.strip()
        if not part:
            continue
        out.append(None if part.lower() == DIRECT else part)
    return out or [None]


def proxy_labels(pool: Iterable[str | None]) -> list[str]:
    """Human-readable labels for a pool (URL or ``direct``)."""
    return [p or DIRECT for p in pool]


class EgressPool:
    """Rotating pool of egress IPs for YouTube fetches.

    Use :meth:`cycle` to drive round-robin rotation across fetches and
    :meth:`next_after` to jump to a fresh IP when the current one is throttled.
    Both return ``(label, spec)`` where ``spec`` is a proxy URL or ``None``.
    """

    def __init__(self, pool: list[str | None] | None = None) -> None:
        self._pool: list[str | None] = pool if pool is not None else parse_proxy_pool()
        if not self._pool:
            self._pool = [None]
        self._rr: Iterator[int] = itertools.cycle(range(len(self._pool)))
        if len(self._pool) > 1:
            logger.info(
                "EgressPool: rotating across %d egress IPs (%s)",
                len(self._pool),
                proxy_labels(self._pool),
            )

    def cycle(self) -> tuple[str, str | None]:
        """Return the next egress ``(label, spec)`` in round-robin order."""
        idx = next(self._rr)
        spec = self._pool[idx]
        return (spec or DIRECT, spec)
