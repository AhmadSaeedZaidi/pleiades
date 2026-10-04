"""Mistral Voxtral speech-to-text transcription.

Sends an extracted audio file to Mistral's ``/v1/audio/transcriptions`` endpoint
(model ``voxtral-mini-latest``) and normalises the response to the vault schema
(``{text, start, duration}`` with times in seconds).
"""

import logging
from pathlib import Path
from typing import Any

import httpx

from maia.scribe.ratelimit import CallPacer

logger = logging.getLogger(__name__)

_ENDPOINT = "https://api.mistral.ai/v1/audio/transcriptions"
_MODEL = "voxtral-mini-latest"
_TIMEOUT = 300.0

# Client-side pacing: never trust the provider's 429 alone. Keep our own calls
# spaced so we stay well under Mistral's per-minute quota.
_pacer = CallPacer(5.0)


class TranscriptionUnavailableError(Exception):
    """Mistral is not configured (no API key)."""


class MistralTranscriptionError(Exception):
    """Mistral transcription failed (API error, empty result, etc.)."""


class MistralRateLimitError(MistralTranscriptionError):
    """Mistral returned HTTP 429 (quota / rate limit)."""


class MistralTranscriber:
    """Transcribe audio files via Mistral Voxtral."""

    def __init__(self, api_key: str | None = None) -> None:
        if api_key is None:
            from atlas.config import settings

            secret = settings.MISTRAL_API_KEY
            api_key = secret.get_secret_value() if secret else None
        if not api_key:
            raise TranscriptionUnavailableError("MISTRAL_API_KEY is not configured")
        self.api_key = api_key
        self.logger = logging.getLogger("maia.scribe.mistral")

    def transcribe(self, audio_path: Path, language: str | None = None) -> list[dict[str, Any]]:
        """Transcribe *audio_path*, returning ``[{text, start, duration}]`` segments.

        Args:
            audio_path: Path to an audio file (opus/mp3/wav/…).
            language: Optional ISO-639-1 hint; omitted → Voxtral auto-detects.

        Raises:
            MistralRateLimitError: on HTTP 429.
            MistralTranscriptionError: on other failures or empty output.
        """
        data: dict[str, str] = {
            "model": _MODEL,
            "timestamp_granularities": "segment",
        }
        if language:
            data["language"] = language

        try:
            with audio_path.open("rb") as fh:
                files = {"file": (audio_path.name, fh, "audio/ogg")}
                _pacer.wait()
                resp = httpx.post(
                    _ENDPOINT,
                    headers={"x-api-key": self.api_key},
                    data=data,
                    files=files,
                    timeout=_TIMEOUT,
                )
        except httpx.HTTPError as e:
            raise MistralTranscriptionError(f"HTTP error calling Mistral: {e}") from e

        if resp.status_code == 429:
            raise MistralRateLimitError("Mistral rate limit / quota exceeded (HTTP 429)")
        if resp.status_code != 200:
            raise MistralTranscriptionError(
                f"Mistral returned HTTP {resp.status_code}: {resp.text[:200]}"
            )

        payload = resp.json()
        segments = self._normalise(payload)
        if not segments:
            raise MistralTranscriptionError("Mistral returned no transcript segments")
        return segments

    @staticmethod
    def _normalise(payload: dict[str, Any]) -> list[dict[str, Any]]:
        """Convert Voxtral ``{text,start,end}`` segments to ``{text,start,duration}``."""
        out: list[dict[str, Any]] = []
        for seg in payload.get("segments", []):
            text = (seg.get("text") or "").strip()
            if not text:
                continue
            start = float(seg.get("start", 0.0) or 0.0)
            end = float(seg.get("end", start) or start)
            out.append({"text": text, "start": start, "duration": max(0.0, end - start)})

        # Fallback: no segments but a flat text field (rare).
        if not out and payload.get("text"):
            out.append({"text": payload["text"].strip(), "start": 0.0, "duration": 0.0})
        return out
