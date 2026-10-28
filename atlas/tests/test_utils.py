"""Tests for utility functions."""

import asyncio

import pytest
from atlas.utils import (
    KeyRing,
    QuotaExhaustedError,
    ResiliencyExecutor,
    YouTubeAPIError,
    validate_channel_id,
    validate_youtube_id,
)


def test_validate_youtube_id_valid():
    """Test valid YouTube video IDs."""
    assert validate_youtube_id("dQw4w9WgXcQ") is True
    assert validate_youtube_id("jNQXAC9IVRw") is True


def test_validate_youtube_id_invalid():
    """Test invalid YouTube video IDs."""
    assert validate_youtube_id("") is False
    assert validate_youtube_id("short") is False
    assert validate_youtube_id("toolongvideoidentifier") is False
    assert validate_youtube_id("invalid!@#") is False
    assert validate_youtube_id(None) is False


def test_validate_channel_id_valid():
    """Test valid YouTube channel IDs."""
    assert validate_channel_id("UCuAXFkgsw1L7xaCfnd5JJOw") is True


def test_validate_channel_id_invalid():
    """Test invalid YouTube channel IDs."""
    assert validate_channel_id("") is False
    assert validate_channel_id("UCshort") is False
    assert validate_channel_id("notstartwithuc_butcorrectlen") is False
    assert validate_channel_id("UC!@#$%^&*()_invalid!!") is False
    assert validate_channel_id(None) is False


@pytest.fixture
def fixed_key_rings(monkeypatch):
    """Pin deterministic key rings so resiliency tests don't depend on env."""

    class _StubSettings:
        key_rings = {
            "hunting": ["k1", "k2", "k3"],
            "tracking": ["k2"],
            "archeology": ["k3"],
        }

    monkeypatch.setattr("atlas.config.settings", _StubSettings)


def test_keyring_sessions_are_unique(fixed_key_rings):
    """Concurrent sessions must not share attempt state (regression test)."""
    kr = KeyRing("hunting")
    s1 = kr.start_session()
    s2 = kr.start_session()
    assert s1 != s2

    # Each session independently starts at the head of the ring.
    assert kr.get_session_key(s1) == "k1"
    assert kr.get_session_key(s2) == "k1"

    # Rotating s1 must not affect s2's key selection.
    assert kr.attempt_rotation(s1) is True
    assert kr.get_session_key(s1) == "k2"
    assert kr.attempt_rotation(s2) is True
    assert kr.get_session_key(s2) == "k2"


def test_resiliency_raises_quota_exhausted_not_sys_exit(fixed_key_rings):
    """Exhaustion raises QuotaExhaustedError, never calls sys.exit."""

    async def always_quota(key: str) -> None:
        raise RuntimeError("quota exceeded 429")

    async def run() -> str:
        ex = ResiliencyExecutor(KeyRing("hunting"), "tester")
        try:
            await ex.execute_async(always_quota)
        except QuotaExhaustedError:
            return "raised"
        except SystemExit:
            return "sysexit"
        return "none"

    assert asyncio.run(run()) == "raised"


def test_is_quota_error_detects_key_and_quota_errors(fixed_key_rings):
    """Legacy untyped 403, 429, and quota strings all trigger rotation.

    Typed YouTube errors use their explicit API reason instead of this fallback.
    """
    ex = ResiliencyExecutor(KeyRing("hunting"), "tester")
    assert ex._is_quota_error(RuntimeError("429 Too Many Requests")) is True
    assert ex._is_quota_error(RuntimeError("quotaExceeded")) is True
    assert ex._is_quota_error(RuntimeError("403 Forbidden: key revoked")) is True


@pytest.mark.parametrize(
    ("status", "reason", "expected"),
    [
        (403, "quotaExceeded", "quota"),
        (403, "dailyLimitExceeded", "quota"),
        (403, "rateLimitExceeded", "quota"),
        (403, "userRateLimitExceeded", "quota"),
        (429, "rateLimitExceeded", "quota"),
        (400, "keyInvalid", "dead_key"),
        (400, "keyExpired", "dead_key"),
        (403, "accessNotConfigured", "fatal"),
        (401, "authError", "fatal"),
        (403, "forbidden", "fatal"),
    ],
)
def test_typed_youtube_error_disposition(fixed_key_rings, status: int, reason: str, expected: str):
    executor = ResiliencyExecutor(KeyRing("hunting"), "tester")

    assert executor._classify_error(YouTubeAPIError(status, reason, "message")) == expected


def test_youtube_api_error_preserves_reason_and_message(fixed_key_rings):
    body = (
        '{"error":{"code":403,"message":"Daily quota reached",'
        '"errors":[{"reason":"quotaExceeded","message":"quota"}]}}'
    )

    error = YouTubeAPIError.from_response(403, body)

    assert error.status == 403
    assert error.reason == "quotaExceeded"
    assert error.message == "Daily quota reached"
    assert "quotaExceeded" in str(error)
    assert "Daily quota reached" in str(error)


def test_quota_error_rotates_without_blacklisting(fixed_key_rings):
    key_ring = KeyRing("hunting")
    executor = ResiliencyExecutor(key_ring, "tester")
    seen: list[str] = []

    async def always_quota(key: str) -> None:
        seen.append(key)
        raise YouTubeAPIError(403, "quotaExceeded", "daily quota reached")

    with pytest.raises(QuotaExhaustedError):
        asyncio.run(executor.execute_async(always_quota))

    assert seen == ["k1", "k2", "k3"]
    assert key_ring._dead_keys == set()


def test_explicit_invalid_key_is_blacklisted_and_rotation_continues(fixed_key_rings):
    key_ring = KeyRing("hunting")
    executor = ResiliencyExecutor(key_ring, "tester")
    seen: list[str] = []

    async def invalid_then_success(key: str) -> dict[str, bool]:
        seen.append(key)
        if key == "k1":
            raise YouTubeAPIError(400, "keyInvalid", "invalid key")
        return {"ok": True}

    result = asyncio.run(executor.execute_async(invalid_then_success))

    assert result == {"ok": True}
    assert seen == ["k1", "k2"]
    assert key_ring._dead_keys == {"k1"}


def test_access_not_configured_fails_fast_without_blacklisting(fixed_key_rings):
    key_ring = KeyRing("hunting")
    executor = ResiliencyExecutor(key_ring, "tester")
    seen: list[str] = []

    async def misconfigured_project(key: str) -> None:
        seen.append(key)
        raise YouTubeAPIError(403, "accessNotConfigured", "API disabled")

    with pytest.raises(YouTubeAPIError) as raised:
        asyncio.run(executor.execute_async(misconfigured_project))

    assert raised.value.reason == "accessNotConfigured"
    assert seen == ["k1"]
    assert key_ring._dead_keys == set()


def test_next_key_never_returns_a_blacklisted_key():
    """Regression: next_key() ignored mark_key_dead().

    The rotation cycle was built from self.keys at construction, so blacklisting
    a revoked key had no effect and it kept being handed out for the lifetime of
    the process.
    """
    ring = KeyRing.from_keys("t", ["k1", "k2", "k3"])
    ring.mark_key_dead("k1")

    assert ring.live_size == 2
    handed_out = [ring.next_key() for _ in range(8)]
    assert "k1" not in handed_out
    assert set(handed_out) == {"k2", "k3"}


def test_next_key_raises_when_every_key_is_blacklisted():
    ring = KeyRing.from_keys("t", ["k1", "k2"])
    ring.mark_key_dead("k1")
    ring.mark_key_dead("k2")

    with pytest.raises(QuotaExhaustedError):
        ring.next_key()


def test_rotation_before_first_fetch_does_not_self_exhaust_a_single_key_ring():
    """Regression: a 1-key ring reported exhaustion without any HTTP request.

    attempt_rotation() used to mark order[0] as seen before a key had ever been
    handed out, so the very first rotation consumed the only key. Nothing in atlas
    or maia calls attempt_rotation before get_session_key, so the workaround was
    dead weight that only created a footgun.
    """
    ring = KeyRing.from_keys("t", ["solo"])
    session = ring.start_session()

    assert ring.attempt_rotation(session) is True
    assert ring.get_session_key(session) == "solo"
    # Now the key really has been used, so rotation must report exhaustion.
    assert ring.attempt_rotation(session) is False
