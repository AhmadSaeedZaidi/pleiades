"""Tunable thresholds for the pre-ingestion quality gate."""

import re
from dataclasses import dataclass

from atlas.config import get_settings


@dataclass(frozen=True)
class QualityThresholds:
    """Tunable gate thresholds."""

    min_duration_seconds: int = 65
    min_views_per_hour: float = 5.0
    min_engagement_rate: float = 0.005
    # Videos younger than this are exempt from the velocity check (too fresh to
    # judge traction); duration + engagement still apply.
    velocity_grace_hours: float = 1.0

    shorts_head_enabled: bool = True
    shorts_head_max_duration: int = 600
    shorts_head_timeout: float = 5.0
    shorts_head_concurrency: int = 8

    ai_patterns: tuple[re.Pattern[str], ...] = ()

    reupload_patterns: tuple[re.Pattern[str], ...] = ()

    min_subscribers: int = 50
    max_videos_per_day: float = 20.0
    max_videos_per_subscriber: float = 5.0

    @classmethod
    def from_settings(cls) -> "QualityThresholds":
        s = get_settings()
        return cls(
            min_duration_seconds=s.QUALITY_MIN_DURATION_SECONDS,
            min_views_per_hour=s.QUALITY_MIN_VIEWS_PER_HOUR,
            min_engagement_rate=s.QUALITY_MIN_ENGAGEMENT_RATE,
            shorts_head_enabled=s.QUALITY_SHORTS_HEAD_ENABLED,
            shorts_head_max_duration=s.QUALITY_SHORTS_HEAD_MAX_DURATION,
            shorts_head_timeout=s.QUALITY_SHORTS_HEAD_TIMEOUT,
            shorts_head_concurrency=s.QUALITY_SHORTS_HEAD_CONCURRENCY,
            ai_patterns=tuple(re.compile(p, re.IGNORECASE) for p in s.QUALITY_AI_DENYLIST),
            reupload_patterns=tuple(
                re.compile(p, re.IGNORECASE) for p in s.QUALITY_REUPLOAD_DENYLIST
            ),
            min_subscribers=s.QUALITY_MIN_SUBSCRIBERS,
            max_videos_per_day=s.QUALITY_MAX_VIDEOS_PER_DAY,
            max_videos_per_subscriber=s.QUALITY_MAX_VIDEOS_PER_SUBSCRIBER,
        )
