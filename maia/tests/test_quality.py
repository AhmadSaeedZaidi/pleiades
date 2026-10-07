"""Tests for the heuristic quality gate (pure logic)."""

from datetime import UTC, datetime, timedelta

from maia.quality import (
    QualityThresholds,
    evaluate_video,
    parse_iso8601_duration,
)


def test_parse_iso8601_duration():
    assert parse_iso8601_duration("PT1M5S") == 65
    assert parse_iso8601_duration("PT1H2M3S") == 3723
    assert parse_iso8601_duration("PT45S") == 45
    assert parse_iso8601_duration("PT1H") == 3600
    assert parse_iso8601_duration("P0D") == 0
    assert parse_iso8601_duration("") == 0
    assert parse_iso8601_duration(None) == 0
    assert parse_iso8601_duration("garbage") == 0


def _item(duration, views, likes, comments, published_at):
    return {
        "contentDetails": {"duration": duration},
        "statistics": {
            "viewCount": str(views),
            "likeCount": str(likes),
            "commentCount": str(comments),
        },
        "snippet": {"publishedAt": published_at},
    }


def _iso(dt):
    return dt.isoformat().replace("+00:00", "Z")


def test_rejects_shorts():
    now = datetime.now(UTC)
    r = evaluate_video(_item("PT45S", 1000, 50, 10, _iso(now - timedelta(hours=10))))
    assert not r.passed
    assert "duration" in r.reason


def test_rejects_low_velocity():
    now = datetime.now(UTC)
    r = evaluate_video(_item("PT5M", 20, 5, 5, _iso(now - timedelta(hours=10))))
    assert not r.passed
    assert "velocity" in r.reason


def test_rejects_low_engagement():
    now = datetime.now(UTC)
    r = evaluate_video(_item("PT5M", 100_000, 10, 10, _iso(now - timedelta(hours=10))))
    assert not r.passed
    assert "engagement" in r.reason


def test_passes_good_video():
    now = datetime.now(UTC)
    r = evaluate_video(_item("PT5M", 100_000, 3000, 500, _iso(now - timedelta(hours=10))))
    assert r.passed
    assert r.views_per_hour > 5
    assert r.engagement_rate > 0.005


def test_fresh_video_exempt_from_velocity():
    """Videos younger than the grace window skip the velocity check."""
    now = datetime.now(UTC)
    # 3 views, 10 min old → would fail velocity, but is exempt; passes on engagement.
    r = evaluate_video(_item("PT5M", 3, 2, 1, _iso(now - timedelta(minutes=10))))
    assert r.passed


def test_zero_views_fails_engagement():
    now = datetime.now(UTC)
    r = evaluate_video(_item("PT5M", 0, 0, 0, _iso(now - timedelta(minutes=10))))
    assert not r.passed


def test_custom_thresholds():
    now = datetime.now(UTC)
    th = QualityThresholds(min_duration_seconds=1, min_views_per_hour=0.0, min_engagement_rate=0.0)
    r = evaluate_video(_item("PT10S", 5, 0, 0, _iso(now - timedelta(hours=5))), th)
    assert r.passed
