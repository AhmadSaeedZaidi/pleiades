"""
Tests for Maia transcription orchestration (scribe captions + singer audio).
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from maia.scribe.grok import GrokRateLimitError, GrokTranscriptionError
from maia.scribe.loader import (
    TranscriptExtractionError,
    TranscriptRateLimitError,
)
from maia.scribe.mistral import (
    MistralRateLimitError,
    TranscriptionUnavailableError,
)
from maia.scribe.transcription import (
    TranscriptResult,
    transcribe_audio_path,
    transcribe_video,
)

FAKE_SEGMENTS = [{"text": "hello", "start": 0.0, "duration": 1.0}]
AUDIO = Path("/tmp/fake.opus")


def _patch_transcribers(grok_side=None, mistral_side=None):
    """Patch the Grok/Mistral transcribers; returns active context managers."""
    grok_inst = MagicMock()
    grok_inst.transcribe = MagicMock(side_effect=grok_side)
    mistral_inst = MagicMock()
    mistral_inst.transcribe = MagicMock(side_effect=mistral_side)
    return (
        patch("maia.scribe.transcription.GrokTranscriber", return_value=grok_inst),
        patch("maia.scribe.transcription.MistralTranscriber", return_value=mistral_inst),
    )


def test_transcribe_audio_path_grok():
    g, m = _patch_transcribers(grok_side=lambda p: FAKE_SEGMENTS)
    with g, m:
        result = transcribe_audio_path(AUDIO, strategy="grok")
    assert isinstance(result, TranscriptResult)
    assert result.source == "grok"
    assert result.segments == FAKE_SEGMENTS


def test_transcribe_audio_path_mistral():
    g, m = _patch_transcribers(mistral_side=lambda p: FAKE_SEGMENTS)
    with g, m:
        result = transcribe_audio_path(AUDIO, strategy="mistral")
    assert result.source == "mistral"


def test_transcribe_audio_path_auto_grok_no_mistral():
    """auto: Grok succeeds → Mistral is not called."""
    g, m = _patch_transcribers(grok_side=lambda p: FAKE_SEGMENTS)
    with g, m as mp:
        result = transcribe_audio_path(AUDIO, strategy="auto")
        mp.return_value.transcribe.assert_not_called()
    assert result.source == "grok"


def test_transcribe_audio_path_auto_falls_back_to_mistral():
    """auto: Grok hard-fails → Mistral is used."""
    g, m = _patch_transcribers(
        grok_side=GrokTranscriptionError("grok down"),
        mistral_side=lambda p: FAKE_SEGMENTS,
    )
    with g, m:
        result = transcribe_audio_path(AUDIO, strategy="auto")
    assert result.source == "mistral"


def test_transcribe_audio_path_rate_limited_raises():
    """Both transcribers rate-limited → TranscriptRateLimitError."""
    g, m = _patch_transcribers(
        grok_side=GrokRateLimitError("429"),
        mistral_side=MistralRateLimitError("429"),
    )
    with g, m:
        with pytest.raises(TranscriptRateLimitError):
            transcribe_audio_path(AUDIO, strategy="auto")


def test_transcribe_audio_path_unavailable_raises():
    """No transcriber configured → TranscriptExtractionError."""
    g, m = _patch_transcribers(
        grok_side=TranscriptionUnavailableError("no grok key"),
        mistral_side=TranscriptionUnavailableError("no mistral key"),
    )
    with g, m:
        with pytest.raises(TranscriptExtractionError):
            transcribe_audio_path(AUDIO, strategy="auto")


def test_transcribe_video_captions_only():
    """Scribe is captions-only: transcribe_video never touches audio."""
    with patch("maia.scribe.transcription.TranscriptLoader") as MockLoader:
        inst = MockLoader.return_value
        inst.fetch = MagicMock(return_value=FAKE_SEGMENTS)
        result = transcribe_video("VIDEO_001")
    assert isinstance(result, TranscriptResult)
    assert result.source == "captions"
    assert result.segments == FAKE_SEGMENTS
    assert result.audio_bytes is None
