"""Per-video and per-channel quality heuristics (pure, side-effect free)."""

import re
from datetime import UTC, datetime
from typing import Any

from maia.quality.duration import parse_iso8601_duration
from maia.quality.thresholds import QualityThresholds
from maia.quality.types import QualityResult


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def evaluate_video(
    item: dict[str, Any],
    thresholds: QualityThresholds | None = None,
    now: datetime | None = None,
) -> QualityResult:
    """Evaluate a ``videos.list`` item against the quality thresholds.

    Args:
        item: A YouTube ``videos.list`` entry with ``snippet``, ``contentDetails``
            and ``statistics`` parts.
        thresholds: Overrides; defaults to values from settings.
        now: Reference time (defaults to ``datetime.now(UTC)``); injectable for tests.

    Returns a :class:`QualityResult`; ``passed`` is False with a reason on the
    first failing check.
    """
    thresholds = thresholds or QualityThresholds()
    now = now or datetime.now(UTC)

    content = item.get("contentDetails", {}) or {}
    stats = item.get("statistics", {}) or {}
    snippet = item.get("snippet", {}) or {}

    duration = parse_iso8601_duration(content.get("duration"))
    views = _to_int(stats.get("viewCount"))
    likes = _to_int(stats.get("likeCount"))
    comments = _to_int(stats.get("commentCount"))

    if duration < thresholds.min_duration_seconds:
        return QualityResult(
            False,
            f"duration {duration}s < {thresholds.min_duration_seconds}s",
            duration_seconds=duration,
            views=views,
        )

    ai_hit = _matches_ai(item, thresholds.ai_patterns)
    if ai_hit:
        return QualityResult(
            False,
            f"ai-slop match: {ai_hit}",
            duration_seconds=duration,
            views=views,
        )

    reupload_hit = _matches_reupload(item, thresholds.reupload_patterns)
    if reupload_hit:
        return QualityResult(
            False,
            f"re-upload match: {reupload_hit}",
            duration_seconds=duration,
            views=views,
        )

    hours_since = _hours_since(snippet.get("publishedAt"), now)
    views_per_hour = views / hours_since if hours_since > 0 else float(views)
    if (
        hours_since >= thresholds.velocity_grace_hours
        and views_per_hour < thresholds.min_views_per_hour
    ):
        return QualityResult(
            False,
            f"velocity {views_per_hour:.2f} v/h < {thresholds.min_views_per_hour}",
            duration_seconds=duration,
            views=views,
            views_per_hour=views_per_hour,
        )

    engagement = (likes + comments) / views if views > 0 else 0.0
    if engagement < thresholds.min_engagement_rate:
        return QualityResult(
            False,
            f"engagement {engagement:.4f} < {thresholds.min_engagement_rate}",
            duration_seconds=duration,
            views=views,
            views_per_hour=views_per_hour,
            engagement_rate=engagement,
        )

    return QualityResult(
        True,
        "ok",
        duration_seconds=duration,
        views=views,
        views_per_hour=views_per_hour,
        engagement_rate=engagement,
    )


def _video_text(item: dict[str, Any]) -> str:
    """Lower-cased title + description + tags for AI-slop scanning."""
    snippet = item.get("snippet", {}) or {}
    parts = [snippet.get("title") or "", snippet.get("description") or ""]
    tags = snippet.get("tags") or []
    if isinstance(tags, list):
        parts.extend(str(tag) for tag in tags)
    return " \n ".join(str(p) for p in parts if p).lower()


def _matches_ai(item: dict[str, Any], patterns: tuple[re.Pattern[str], ...]) -> str | None:
    """Return the matching denylist pattern (or ``None``) for an AI-slop video."""
    if not patterns:
        return None
    text = _video_text(item)
    for pat in patterns:
        if pat.search(text):
            return pat.pattern
    return None


def _matches_reupload(item: dict[str, Any], patterns: tuple[re.Pattern[str], ...]) -> str | None:
    """Return the matching denylist pattern (or ``None``) for a re-uploaded video.

    Scans title, description, and tags for indicators of copyright-infringing
    re-uploads (e.g. emojified network names like ``🅼🆂🅽🅱🅲`` for MSNBC).
    These channels get DMCA'd within hours, so the tracker finds dead videos.
    """
    if not patterns:
        return None
    text = _video_text(item)
    for pat in patterns:
        if pat.search(text):
            return pat.pattern
    return None


def _hours_since(published_at: str | None, now: datetime) -> float:
    if not published_at:
        return 0.0
    try:
        pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    return max(0.0, (now - pub).total_seconds() / 3600.0)


def _channel_age_days(published_at: str | None, now: datetime) -> float:
    if not published_at:
        return 0.0
    try:
        pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    return max(0.0, (now - pub).total_seconds() / 86400.0)


def evaluate_channel(
    channel_id: str | None,
    stats: dict[str, Any] | None,
    thresholds: QualityThresholds,
    now: datetime,
) -> tuple[bool, str]:
    """Reject suspected AI-farm / spam channels via statistics.

    * subscriber floor — tiny channels dumping AI-slop.
    * upload-rate proxy — ``videoCount / channel_age_days`` over a threshold
      indicates mass automated uploading.
    """
    if not channel_id or not stats:
        return True, ""
    subs = stats.get("subscriber_count")
    if thresholds.min_subscribers and subs is not None and subs < thresholds.min_subscribers:
        return False, f"channel subs {subs} < {thresholds.min_subscribers}"

    if thresholds.max_videos_per_day and stats.get("video_count"):
        age = _channel_age_days(stats.get("published_at"), now)
        if age > 1.0:  # need a meaningful age to estimate a rate
            rate = stats["video_count"] / age
            if rate > thresholds.max_videos_per_day:
                return (
                    False,
                    f"channel upload rate {rate:.1f}/day > {thresholds.max_videos_per_day}",
                )

    # Subscriber-density signal: far more videos than subscribers is the
    # hallmark of spam/AI farms and is robust to legit high-volume networks
    # (which have many subscribers per video). Normalises volume by audience.
    if thresholds.max_videos_per_subscriber and subs:
        density = stats.get("video_count", 0) / max(subs, 1)
        if density > thresholds.max_videos_per_subscriber:
            return (
                False,
                f"channel videos/sub ratio {density:.1f} > {thresholds.max_videos_per_subscriber}",
            )
    return True, ""
