"""Enrichment + batch filtering used by the discovery agents (Hunter/Archeologist)."""

import asyncio
from datetime import UTC, datetime
from typing import Any

import aiohttp

from maia.quality.gates import _to_int, evaluate_channel, evaluate_video
from maia.quality.shorts import is_youtube_short
from maia.quality.thresholds import QualityThresholds
from maia.quality.types import QualityResult
from maia.utils import video_id_of


async def _load_channel_stats(
    channel_ids: list[str], executor: Any | None, logger: Any | None = None
) -> dict[str, dict[str, Any]]:
    """Resolve channel statistics for the given channel IDs via ``channels.list``.

    Returns a mapping of ``channel_id -> {subscriber_count, video_count,
    published_at}``.

    A failure here is now raised, not swallowed. It used to ``return {}``, and
    ``evaluate_channel`` treats a missing stats dict as "pass" — so a single
    ``channels.list`` error silently disabled one of the three documented gate
    passes for a whole page, with *no log line at all*. Callers now fail the page
    (fail-closed) instead of admitting unfiltered videos.
    """
    if not channel_ids or executor is None:
        return {}
    from atlas.youtube import lookup_channels

    items = await lookup_channels(list(channel_ids), parts="snippet,statistics", executor=executor)

    out: dict[str, dict[str, Any]] = {}
    for it in items:
        cid = it.get("id")
        if not cid:
            continue
        stats = it.get("statistics", {}) or {}
        out[cid] = {
            "subscriber_count": _to_int(stats.get("subscriberCount")),
            "video_count": _to_int(stats.get("videoCount")),
            "published_at": (it.get("snippet", {}) or {}).get("publishedAt"),
        }
    if logger is not None and not out:
        logger.warning(
            "channels.list returned no usable statistics for %d channel(s); "
            "the channel gate cannot be evaluated",
            len(channel_ids),
        )
    return out


async def filter_by_quality(
    search_items: list[dict[str, Any]],
    executor: Any | None,
    enabled: bool | None = None,
    logger: Any | None = None,
    thresholds: QualityThresholds | None = None,
) -> list[dict[str, Any]]:
    """Enrich search results via ``videos.list`` and keep only passing videos.

    Search snippets lack ``statistics``/``contentDetails``, so they are resolved
    (1 quota unit per 50 videos) and filtered via :func:`evaluate_video`. If the
    gate is disabled or there is no executor, raw search items are returned
    unchanged. ``QuotaExhaustedError`` propagates to the caller.
    """
    from atlas.config import get_settings
    from atlas.youtube import lookup_videos

    if enabled is None:
        enabled = get_settings().QUALITY_GATE_ENABLED

    ids: list[str] = []
    for it in search_items:
        vid = it.get("id")
        if isinstance(vid, dict):
            vid = vid.get("videoId")
        if vid:
            ids.append(vid)

    if not ids or not enabled or executor is None:
        return search_items

    enriched = await lookup_videos(ids, executor=executor)
    # Honour an explicitly supplied threshold set. This used to be assigned
    # unconditionally, shadowing the `thresholds` argument, so the parameter was
    # dead API — and tests/test_filtering.py passed an explicit set that happened
    # to match the config defaults, which is why the bug survived.
    if thresholds is None:
        thresholds = QualityThresholds.from_settings()
    now = datetime.now(UTC)

    evaluated: list[tuple[dict[str, Any], QualityResult]] = []
    rejected = 0
    for item in enriched:
        result = evaluate_video(item, thresholds)
        if result.passed:
            evaluated.append((item, result))
        else:
            rejected += 1
            if logger:
                logger.debug(f"Quality gate rejected {video_id_of(item)}: {result.reason}")

    if thresholds.shorts_head_enabled and evaluated:
        candidates = [
            (it, res)
            for it, res in evaluated
            if res.duration_seconds < thresholds.shorts_head_max_duration
        ]
        if candidates:
            sem = asyncio.Semaphore(thresholds.shorts_head_concurrency)
            timeout = thresholds.shorts_head_timeout

            async def _probe(
                it: dict[str, Any], res: QualityResult, session: aiohttp.ClientSession
            ) -> tuple[dict[str, Any], QualityResult] | None:
                vid = video_id_of(it)
                if not vid:
                    return (it, res)
                async with sem:
                    is_short = await is_youtube_short(vid, timeout=timeout, session=session)
                # Unknown (None) is treated as "not a Short" to avoid over-rejecting.
                return None if is_short else (it, res)

            # `session` is passed as an argument rather than captured: it is a
            # local of this function, and relying on the closure over a variable
            # assigned after the def would break the moment anyone hoisted the
            # gather out of the `async with`.
            async with aiohttp.ClientSession() as session:
                probed = await asyncio.gather(*(_probe(it, res, session) for it, res in candidates))
            kept = [p for p in probed if p is not None]
            long_form = [
                (it, res)
                for it, res in evaluated
                if res.duration_seconds >= thresholds.shorts_head_max_duration
            ]
            shorts_hits = len(candidates) - len(kept)
            rejected += shorts_hits
            evaluated = long_form + kept
            if logger and shorts_hits:
                logger.info(f"Quality gate: rejected {shorts_hits} Shorts via HEAD probe")

    final: list[dict[str, Any]] = []
    if evaluated:
        channel_ids = list({it.get("snippet", {}).get("channelId") for it, _ in evaluated} - {None})
        stats_map = await _load_channel_stats(channel_ids, executor, logger)
        for it, _ in evaluated:
            cid = it.get("snippet", {}).get("channelId")
            ok, reason = evaluate_channel(cid, stats_map.get(cid), thresholds, now)
            if ok:
                final.append(it)
            else:
                rejected += 1
                if logger:
                    logger.debug(f"Quality gate rejected {video_id_of(it)} (channel): {reason}")
    passing = list(final)
    if logger:
        logger.info(
            f"Quality gate: {len(passing)} passed / {rejected} rejected / {len(enriched)} enriched"
        )
    return passing
