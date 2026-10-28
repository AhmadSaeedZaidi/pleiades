"""Transcription orchestration.

The Scribe fetches captions only (no STT-quota cost); the Singer transcribes
audio that the streamer already extracted and stored. Keeping audio extraction
in the streamer means each video is fetched from YouTube exactly once.
"""

import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from maia.scribe.audio import AudioExtractionError, download_to_tempfile
from maia.scribe.grok import (
    GrokRateLimitError,
    GrokTranscriber,
    GrokTranscriptionError,
)
from maia.scribe.loader import (
    TranscriptExtractionError,
    TranscriptLoader,
    TranscriptRateLimitError,
)
from maia.scribe.mistral import (
    MistralRateLimitError,
    MistralTranscriber,
    MistralTranscriptionError,
    TranscriptionUnavailableError,
)

logger = logging.getLogger(__name__)


@dataclass
class TranscriptResult:
    """Result of a transcription attempt."""

    segments: list[dict[str, Any]]
    source: str  # "captions" | "mistral" | "grok"
    audio_bytes: bytes | None = None
    audio_ext: str = "opus"


def _transcribe_via_mistral_path(audio_path: Path) -> TranscriptResult:
    """Transcribe a local audio file with Voxtral."""
    segments = MistralTranscriber().transcribe(audio_path)
    return TranscriptResult(segments, "mistral")


def _transcribe_via_grok_path(audio_path: Path) -> TranscriptResult:
    """Transcribe a local audio file with Grok STT."""
    segments = GrokTranscriber().transcribe(audio_path)
    return TranscriptResult(segments, "grok")


def transcribe_audio_path(
    audio_path: Path,
    strategy: str | None = None,
    language: str | None = None,
) -> TranscriptResult:
    """Transcribe a *local* audio file per the configured strategy.

    Used by the **singer** consumer, which fetches the audio the streamer stored
    in the vault. Strategy (``settings.SCRIBE_TRANSCRIBER``): ``grok`` (Grok only),
    ``mistral`` (Voxtral only), or ``auto`` (Grok first, Mistral fallback).
    Raises ``TranscriptRateLimitError`` if all paths were rate-limited, or
    ``TranscriptExtractionError`` if no transcript could be produced.
    """
    from atlas.config import get_settings

    settings = get_settings()
    strategy = strategy or settings.SCRIBE_TRANSCRIBER

    if strategy == "mistral":
        return _mistral_only_path(audio_path)
    if strategy == "grok":
        return _grok_only_path(audio_path)

    # auto: Grok first, Mistral fallback.
    try:
        return _transcribe_via_grok_path(audio_path)
    except GrokRateLimitError:
        logger.info("audio→Grok rate-limited, falling back to audio→Mistral")
    except (TranscriptionUnavailableError, GrokTranscriptionError):
        pass

    try:
        return _transcribe_via_mistral_path(audio_path)
    except TranscriptionUnavailableError:
        raise TranscriptExtractionError("No audio transcriber available for local audio") from None
    except MistralRateLimitError as e:
        raise TranscriptRateLimitError(f"Mistral rate-limited: {e}") from e
    except (AudioExtractionError, MistralTranscriptionError) as e:
        raise TranscriptExtractionError(f"Audio transcription failed: {e}") from e


def _mistral_only_path(audio_path: Path) -> TranscriptResult:
    try:
        return _transcribe_via_mistral_path(audio_path)
    except TranscriptionUnavailableError as e:
        raise TranscriptExtractionError(str(e)) from e
    except MistralRateLimitError as e:
        raise TranscriptRateLimitError(f"Mistral rate-limited: {e}") from e
    except (AudioExtractionError, MistralTranscriptionError) as e:
        raise TranscriptExtractionError(f"Audio transcription failed: {e}") from e


def _grok_only_path(audio_path: Path) -> TranscriptResult:
    try:
        return _transcribe_via_grok_path(audio_path)
    except TranscriptionUnavailableError as e:
        raise TranscriptExtractionError(str(e)) from e
    except GrokRateLimitError as e:
        raise TranscriptRateLimitError(f"Grok rate-limited: {e}") from e
    except (AudioExtractionError, GrokTranscriptionError) as e:
        raise TranscriptExtractionError(f"Audio transcription failed: {e}") from e


def transcribe_video(
    video_id: str,
    strategy: str | None = None,
    store_audio: bool | None = None,
) -> TranscriptResult:
    """Transcribe *video_id* via official/auto **captions** only (Scribe).

    The Scribe is captions-only by design: audio is extracted and stored by the
    streamer, then transcribed by the singer, keeping the Scribe free of
    STT-quota cost and rate limits.

    Raises:
        TranscriptRateLimitError: captions were throttled on every client.
        TranscriptExtractionError: no captions available for this video.
    """
    from atlas.config import get_settings

    settings = get_settings()
    strategy = strategy or settings.SCRIBE_TRANSCRIBER

    if strategy not in ("captions", "auto", "mistral", "grok"):
        logger.warning(f"Unknown SCRIBE_TRANSCRIBER={strategy!r}; using captions")
    # Captions cascade across player clients; returns [{text, start, duration}].
    return TranscriptResult(TranscriptLoader().fetch(video_id), "captions")


def transcribe_audio_download(
    video_id: str,
    strategy: str | None = None,
    store_audio: bool = False,
) -> TranscriptResult:
    """Download *video_id*'s audio then run the Grok→Mistral cascade.

    Used only by standalone tooling not part of the streamer/singer fleet.
    """
    from atlas.config import get_settings

    settings = get_settings()
    strategy = strategy or settings.SCRIBE_TRANSCRIBER

    audio_path, tmpdir = download_to_tempfile(video_id)
    try:
        if strategy in ("captions",):
            raise TranscriptExtractionError("captions strategy has no audio path")
        if strategy == "mistral":
            result = _mistral_only_path(audio_path)
        elif strategy == "grok":
            result = _grok_only_path(audio_path)
        else:
            result = transcribe_audio_path(audio_path)
        if store_audio:
            result.audio_bytes = Path(audio_path).read_bytes()
        return result
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
