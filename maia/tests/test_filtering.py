"""Tests for the Hunter quality gate: Shorts HEAD probe, AI denylist, channel gate."""

import re
from datetime import UTC, datetime, timedelta

import pytest
from maia.quality import (
    QualityThresholds,
    _matches_ai,
    evaluate_channel,
    evaluate_video,
    filter_by_quality,
    is_youtube_short,
)


def _item(duration, views, likes, comments, published_at, **snippet_extra):
    item = {
        "id": snippet_extra.pop("id", "VID"),
        "contentDetails": {"duration": duration},
        "statistics": {
            "viewCount": str(views),
            "likeCount": str(likes),
            "commentCount": str(comments),
        },
        "snippet": {"publishedAt": published_at, "channelId": "CH"},
    }
    item["snippet"].update(snippet_extra)
    return item


def _iso(dt):
    return dt.isoformat().replace("+00:00", "Z")


AI_PATTERNS = tuple(re.compile(p, re.IGNORECASE) for p in [r"made with ai", r"ai generated"])


def test_matches_ai_denylist():
    assert _matches_ai({"snippet": {"title": "This was made with AI"}}, AI_PATTERNS)
    assert _matches_ai({"snippet": {"description": "an ai generated video"}}, AI_PATTERNS)
    assert _matches_ai({"snippet": {"tags": ["ai generated", "slop"]}}, AI_PATTERNS)
    # A video merely *about* AI should not match.
    assert _matches_ai({"snippet": {"title": "Understanding AI in 2025"}}, AI_PATTERNS) is None
    assert _matches_ai({"snippet": {"title": "normal video"}}, ()) is None


def test_evaluate_video_rejects_ai():
    now = datetime.now(UTC)
    th = QualityThresholds(ai_patterns=AI_PATTERNS)
    r = evaluate_video(
        _item("PT5M", 100_000, 3000, 500, _iso(now - timedelta(hours=10)), title="made with AI"),
        th,
    )
    assert not r.passed
    assert "ai-slop" in r.reason


def test_evaluate_channel_min_subscribers():
    th = QualityThresholds(min_subscribers=50, max_videos_per_day=0.0)
    now = datetime.now(UTC)
    ok, _ = evaluate_channel("CH", {"subscriber_count": 10, "video_count": 5}, th, now)
    assert not ok
    ok, _ = evaluate_channel("CH", {"subscriber_count": 5000, "video_count": 5}, th, now)
    assert ok


def test_evaluate_channel_upload_rate():
    th = QualityThresholds(min_subscribers=0, max_videos_per_day=10.0)
    now = datetime.now(UTC)
    # 4000 videos over 200 days = 20/day → rejected.
    ok, reason = evaluate_channel(
        "CH",
        {
            "subscriber_count": None,
            "video_count": 4000,
            "published_at": _iso(now - timedelta(days=200)),
        },
        th,
        now,
    )
    assert not ok
    assert "upload rate" in reason
    # 100 videos over 200 days = 0.5/day → fine.
    ok, _ = evaluate_channel(
        "CH",
        {
            "subscriber_count": None,
            "video_count": 100,
            "published_at": _iso(now - timedelta(days=200)),
        },
        th,
        now,
    )
    assert ok


class _FakeResp:
    def __init__(self, status):
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _FakeSession:
    def __init__(self, status_map):
        self._status_map = status_map

    def head(self, url, **kw):
        vid = url.rstrip("/").split("/")[-1]
        return _FakeResp(self._status_map.get(vid, 303))


@pytest.mark.asyncio
async def test_is_youtube_short():
    assert (
        await is_youtube_short("SHORT1", timeout=1, session=_FakeSession({"SHORT1": 200})) is True
    )  # noqa: E501
    assert await is_youtube_short("LONG1", timeout=1, session=_FakeSession({"LONG1": 303})) is False
    # Odd status / network error → unknown (not rejected).
    assert await is_youtube_short("X", timeout=1, session=_FakeSession({"X": 500})) is None


@pytest.mark.asyncio
async def test_filter_rejects_short_and_ai_and_lowsub(monkeypatch):
    good = _item(
        "PT5M",
        100_000,
        3000,
        500,
        _iso(datetime.now(UTC) - timedelta(hours=10)),
        id="GOOD",
        title="A real documentary",
        channelId="CH_GOOD",
    )
    ai = _item(
        "PT5M",
        100_000,
        3000,
        500,
        _iso(datetime.now(UTC) - timedelta(hours=10)),
        id="AI",
        title="made with AI voiceover",
        channelId="CH_AI",
    )
    short = _item(
        "PT30S",
        100,
        5,
        2,
        _iso(datetime.now(UTC) - timedelta(hours=10)),
        id="SHORT",
        title="quick clip",
        channelId="CH_SHORT",
    )
    lowsub = _item(
        "PT5M",
        100_000,
        3000,
        500,
        _iso(datetime.now(UTC) - timedelta(hours=10)),
        id="LOW",
        title="spam channel",
        channelId="CH_LOW",
    )

    async def fake_videos(ids, executor=None, **kw):
        return [good, ai, short, lowsub]

    async def fake_channels(ids, parts=None, executor=None, **kw):
        def _chan(cid, subs):
            return {
                "id": cid,
                "snippet": {"publishedAt": _iso(datetime.now(UTC) - timedelta(days=2000))},
                "statistics": {"subscriberCount": str(subs), "videoCount": "100"},
            }

        return [
            _chan("CH_GOOD", 50000),
            _chan("CH_AI", 5000),
            _chan("CH_SHORT", 5000),
            _chan("CH_LOW", 5),
        ]

    monkeypatch.setattr("atlas.youtube.lookup_videos", fake_videos)
    monkeypatch.setattr("atlas.youtube.lookup_channels", fake_channels)

    thresholds = QualityThresholds(
        shorts_head_enabled=True,
        shorts_head_max_duration=600,
        ai_patterns=AI_PATTERNS,
        min_subscribers=50,
        max_videos_per_day=10.0,
    )

    class _FakeSessionShorts:
        def __init__(self):
            pass

        def head(self, url, **kw):
            vid = url.rstrip("/").split("/")[-1]
            return _FakeResp(200 if vid == "SHORT" else 303)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr("maia.quality.aiohttp.ClientSession", _FakeSessionShorts)

    passed = await filter_by_quality(
        [good, ai, short, lowsub], executor=object(), thresholds=thresholds, logger=None
    )
    assert {i["id"] for i in passed} == {"GOOD"}
