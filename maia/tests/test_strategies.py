"""Tests for Maia's structured YouTube Data API errors."""

from unittest.mock import patch

import pytest
from atlas.utils import KeyRing, YouTubeAPIError
from maia.strategies import YouTubeSearchStrategy


class _Response:
    status = 403

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        return None

    async def text(self) -> str:
        return (
            '{"error":{"code":403,"message":"Daily quota reached",'
            '"errors":[{"reason":"quotaExceeded","message":"quota"}]}}'
        )


class _Session:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        return None

    def get(self, *args, **kwargs):
        return _Response()


class _Executor:
    async def execute_async(self, request_func):
        return await request_func("test-key")


@pytest.mark.asyncio
async def test_execute_get_preserves_structured_youtube_error():
    ring = KeyRing.from_keys("test", ["test-key"])
    strategy = YouTubeSearchStrategy("test", "test", key_ring=ring)
    strategy.executor = _Executor()

    assert strategy.keys is ring

    with patch("maia.strategies.aiohttp.ClientSession", return_value=_Session()):
        with pytest.raises(YouTubeAPIError) as raised:
            await strategy.execute_get("https://example.invalid", {"part": "snippet"})

    assert raised.value.status == 403
    assert raised.value.reason == "quotaExceeded"
    assert raised.value.message == "Daily quota reached"
