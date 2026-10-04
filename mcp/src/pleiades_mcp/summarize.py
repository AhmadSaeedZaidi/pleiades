"""Transcript summarization via the Mistral chat API (generous free tier).

Builds a concise, structured summary of a transcript (or caption list) for an
LLM to consume. Uses ``mistral-small-latest`` by default; the API key comes from
``MISTRAL_API_KEY`` (Atlas already reads this for Voxtral STT, so the MCP server
reuses the same env var).
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
from atlas.config import settings

logger = logging.getLogger("pleiades_mcp.summarize")

_ENDPOINT = "https://api.mistral.ai/v1/chat/completions"
_MODEL = "mistral-small-latest"
_TIMEOUT = 120.0

_SYSTEM = (
    "You are a meticulous research assistant. Summarize the provided video "
    "transcript into a structured briefing for an analyst. Produce:\n"
    "1. A one-paragraph overview (what the video is about).\n"
    "2. 5-8 key bullet points of concrete claims, facts, or events.\n"
    "3. Any notable entities (people, organizations, places, products) mentioned.\n"
    "4. The overall sentiment/tone (e.g. neutral, critical, promotional).\n"
    "Be faithful to the transcript; do not invent details not present. If the "
    "transcript is empty or unavailable, say so plainly."
)


class SummarizationUnavailableError(Exception):
    """Mistral is not configured (no API key)."""


class SummarizationError(Exception):
    """Mistral summarization failed."""


def _api_key() -> str:
    secret = settings.MISTRAL_API_KEY
    if not secret:
        raise SummarizationUnavailableError("MISTRAL_API_KEY is not configured")
    return secret.get_secret_value()


def _segments_to_text(segments: list[dict[str, Any]]) -> str:
    """Flatten ``[{text, ...}]`` caption/transcript segments into plain text."""
    parts = []
    for seg in segments:
        text = (seg.get("text") or "").strip()
        if text:
            parts.append(text)
    return "\n".join(parts)


def summarize_transcript(
    segments: list[dict[str, Any]],
    *,
    title: str | None = None,
    max_chars: int = 60_000,
) -> str:
    """Return a structured Markdown summary of *segments* using Mistral chat.

    Args:
        segments: transcript/caption segments (``{text, start, duration}``).
        title: optional video title to give the model context.
        max_chars: hard cap on transcript characters sent to the model.

    Raises:
        SummarizationUnavailableError: Mistral key missing.
        SummarizationError: API failure or empty result.
    """
    transcript = _segments_to_text(segments)
    if not transcript.strip():
        raise SummarizationError("Transcript is empty; nothing to summarize")

    if len(transcript) > max_chars:
        logger.warning(
            f"Transcript truncated from {len(transcript)} to {max_chars} chars for summary"
        )
        transcript = transcript[:max_chars]

    user = transcript
    if title:
        user = f"Video title: {title}\n\nTranscript:\n{transcript}"

    api_key = _api_key()
    try:
        resp = httpx.post(
            _ENDPOINT,
            headers={"x-api-key": api_key, "content-type": "application/json"},
            json={
                "model": _MODEL,
                "messages": [
                    {"role": "system", "content": _SYSTEM},
                    {"role": "user", "content": user},
                ],
                "temperature": 0.2,
            },
            timeout=_TIMEOUT,
        )
    except httpx.HTTPError as e:
        raise SummarizationError(f"HTTP error calling Mistral: {e}") from e

    if resp.status_code == 429:
        raise SummarizationError("Mistral rate limit / quota exceeded (HTTP 429)")
    if resp.status_code != 200:
        raise SummarizationError(f"Mistral returned HTTP {resp.status_code}: {resp.text[:200]}")

    payload = resp.json()
    try:
        summary = payload["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as e:
        raise SummarizationError(f"Unexpected Mistral response shape: {e}") from e

    if not summary or not summary.strip():
        raise SummarizationError("Mistral returned an empty summary")
    return summary.strip()
