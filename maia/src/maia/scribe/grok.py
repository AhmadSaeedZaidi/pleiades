"""Groq speech-to-text transcription (OpenAI-Whisper compatible).

Sends an extracted audio file to Groq's ``/openai/v1/audio/transcriptions``
endpoint (model ``whisper-large-v3``) and normalises the response to the vault
schema (``{text, start, duration}`` with times in seconds). This replaced
Mistral Voxtral as the audio-fallback transcriber after Voxtral's quota ran out.

Groq keys are issued from console.groq.com (``gsk_…``) and are *not* xAI keys,
so the endpoint is ``api.groq.com``, not ``api.x.ai``.
"""

import logging
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_ENDPOINT = "https://api.groq.com/openai/v1/audio/transcriptions"
_MODEL = "whisper-large-v3"
_TIMEOUT = 300.0


class TranscriptionUnavailableError(Exception):
    """Groq is not configured (no API key)."""


class GrokTranscriptionError(Exception):
    """Groq transcription failed (API error, empty result, etc.)."""


class GrokRateLimitError(GrokTranscriptionError):
    """Groq returned HTTP 429 (quota / rate limit)."""


class GrokTranscriber:
    """Transcribe audio files via the Groq Whisper endpoint."""

    def __init__(self, api_key: str | None = None) -> None:
        if api_key is None:
            from atlas.config import settings

            secret = settings.GROK_API_KEY
            api_key = secret.get_secret_value() if secret else None
        if not api_key:
            raise TranscriptionUnavailableError("GROK_API_KEY is not configured")
        self.api_key = api_key
        self.logger = logging.getLogger("maia.scribe.grok")

    def transcribe(self, audio_path: Path, language: str | None = None) -> list[dict[str, Any]]:
        """Transcribe *audio_path*, returning ``[{text, start, duration}]`` segments.

        Args:
            audio_path: Path to an audio file (opus/mp3/wav/…).
            language: Optional ISO-639-1 hint.

        Raises:
            GrokRateLimitError: on HTTP 429.
            GrokTranscriptionError: on other failures or empty output.
        """
        data: dict[str, str] = {"model": _MODEL}
        if language:
            data["language"] = language

        try:
            with audio_path.open("rb") as fh:
                files = {"file": (audio_path.name, fh, "audio/ogg")}
                resp = httpx.post(
                    _ENDPOINT,
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    data=data,
                    files=files,
                    timeout=_TIMEOUT,
                )
        except httpx.HTTPError as e:
            raise GrokTranscriptionError(f"HTTP error calling Groq STT: {e}") from e

        if resp.status_code == 429:
            raise GrokRateLimitError("Groq STT rate limit / quota exceeded (HTTP 429)")
        if resp.status_code != 200:
            raise GrokTranscriptionError(
                f"Groq STT returned HTTP {resp.status_code}: {resp.text[:200]}"
            )

        try:
            payload = resp.json()
        except ValueError as e:
            raise GrokTranscriptionError(f"Groq STT returned non-JSON body: {e}") from e

        segments = self._normalise(payload)
        if not segments:
            raise GrokTranscriptionError("Groq STT returned no transcript segments")
        return segments

    @staticmethod
    def _normalise(payload: dict[str, Any]) -> list[dict[str, Any]]:
        """Normalise a Groq/OpenAI Whisper response into ``{text,start,duration}``.

        Honours ``segments`` (when ``response_format=verbose_json``) and falls
        back to the flat ``text`` field.
        """
        segments = payload.get("segments") or []
        if segments:
            out: list[dict[str, Any]] = []
            for seg in segments:
                text = (seg.get("text") or "").strip()
                if not text:
                    continue
                start = float(seg.get("start", 0.0) or 0.0)
                end = float(seg.get("end", start) or start)
                out.append({"text": text, "start": start, "duration": max(0.0, end - start)})
            if out:
                return out

        text = (payload.get("text") or "").strip()
        if text:
            return [{"text": text, "start": 0.0, "duration": 0.0}]
        return []
