"""Result type for the pre-ingestion quality gate."""

from dataclasses import dataclass


@dataclass(frozen=True)
class QualityResult:
    """Outcome of evaluating one video."""

    passed: bool
    reason: str
    duration_seconds: int = 0
    views: int = 0
    views_per_hour: float = 0.0
    engagement_rate: float = 0.0
