"""Shorts detection via a lightweight HEAD probe to ``/shorts/{video_id}``."""

import aiohttp

from maia.quality.duration import _REDIRECT_STATUS


async def is_youtube_short(
    video_id: str,
    *,
    timeout: float,
    session: aiohttp.ClientSession,
) -> bool | None:
    """Probe ``/shorts/{video_id}`` with a HEAD request (no body download).

    Returns ``True`` if the video is a Short (200 OK), ``False`` if it is
    long-form (3xx redirect to ``/watch``), or ``None`` on network/odd errors
    (treated as "unknown" so we do not over-reject).
    """
    url = f"https://www.youtube.com/shorts/{video_id}"
    try:
        async with session.head(
            url, allow_redirects=False, timeout=aiohttp.ClientTimeout(total=timeout)
        ) as resp:
            if resp.status == 200:
                return True
            if resp.status in _REDIRECT_STATUS:
                return False
            return None
    except Exception:
        return None
