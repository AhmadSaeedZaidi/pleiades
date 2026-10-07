"""Strategy pattern for YouTube Data API scraping.

Encapsulates core HTTP request, key rotation, and rate-limit backoff logic
shared by the Hunter and Archeologist producers, plus the Tracker consumer.
"""

import logging
from typing import Any

import aiohttp
from atlas.utils import KeyRing, ResiliencyExecutor, YouTubeAPIError

logger = logging.getLogger(__name__)


class YouTubeSearchStrategy:
    """Base class for YouTube Data API search and video resolution.

    Provides ``execute_get()`` with key rotation and rate-limit handling.
    Raises ``QuotaExhaustedError`` when all API keys are exhausted.
    """

    SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
    VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"

    def __init__(
        self,
        key_ring_pool: str,
        agent_name: str,
        *,
        key_ring: KeyRing | None = None,
    ) -> None:
        self.keys = key_ring if key_ring is not None else KeyRing(key_ring_pool)
        self.executor = ResiliencyExecutor(self.keys, agent_name=agent_name)

    async def execute_get(self, url: str, params: dict[str, Any]) -> dict[str, Any] | None:
        """Execute an authenticated GET with key rotation and rate-limit handling.

        Returns the parsed JSON body, or raises ``QuotaExhaustedError`` when
        all API keys are exhausted.
        """

        async def make_request(api_key: str) -> dict[str, Any]:
            params_with_key: dict[str, Any] = {**params, "key": api_key}
            async with (
                aiohttp.ClientSession() as session,
                session.get(url, params=params_with_key) as resp,
            ):
                if resp.status == 200:
                    result: dict[str, Any] = await resp.json()
                    return result
                error_text = await resp.text()
                logger.error(f"HTTP {resp.status} for {url}: {error_text[:200]}")
                raise YouTubeAPIError.from_response(resp.status, error_text)

        result = await self.executor.execute_async(make_request)
        if result is None:
            raise RuntimeError(f"YouTube API request failed (retries exhausted) for {url}")
        return dict[str, Any](result)

    async def search(self, params: dict[str, Any]) -> dict[str, Any] | None:
        """Perform a ``youtube/v3/search`` request with the given params."""
        return await self.execute_get(self.SEARCH_URL, params)

    async def fetch_videos(
        self, video_ids: list[str], parts: str = "statistics"
    ) -> dict[str, Any] | None:
        """Resolve statistics/details for a batch of video IDs (max 50 per call)."""
        params: dict[str, Any] = {"part": parts, "id": ",".join(video_ids)}
        return await self.execute_get(self.VIDEOS_URL, params)
