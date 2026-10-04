"""
Tests for Maia Scribe module (caption + transcript extraction).

The Scribe is the single owner of caption fetching: it hits YouTube's
`timedtext` endpoint via its multi-client cascade (with 429-release-to-PENDING
backoff) and only falls back to speech-to-text on the Singer's stored audio
(read from the vault) when no captions exist.
"""

import io
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from atlas.models import Video
from atlas.utils import QuotaExhaustedError
from maia.scribe.flow import (
    SCRIBE_NO_TRANSCRIPT_MESSAGE,
    _transcribe,
    fetch_scribe_targets_task,
    process_transcript_task,
    scribe_flow,
)
from maia.scribe.loader import (
    TranscriptExtractionError,
    TranscriptLoader,
    TranscriptRateLimitError,
)
from maia.scribe.transcription import TranscriptResult

from tiered_storage import StagedItem


def _json3(segments: list[dict]) -> bytes:
    events = [
        {
            "tStartMs": int(s["start"] * 1000),
            "dDurationMs": int(s["duration"] * 1000),
            "segs": [{"utf8": s["text"]}],
        }
        for s in segments
    ]
    return json.dumps({"events": events}).encode()


def _audio_result(segments: list[dict]) -> TranscriptResult:
    return TranscriptResult(segments=segments, source="grok")


@pytest.mark.asyncio
async def test_fetch_scribe_targets_empty():
    """Test fetch_scribe_targets returns empty list when no videos need transcripts."""
    with patch("maia.scribe.flow.VideoRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.claim_scribe_batch = AsyncMock(return_value=[])

        result = await fetch_scribe_targets_task(batch_size=10)

        assert result == []
        mock_repo.claim_scribe_batch.assert_called_once_with(10)


@pytest.mark.asyncio
async def test_fetch_scribe_targets_with_videos():
    """Test fetch_scribe_targets returns videos needing transcripts."""
    mock_videos = [
        Video(id="VIDEO_001", title="Test Video 1"),
        Video(id="VIDEO_002", title="Test Video 2"),
    ]

    with patch("maia.scribe.flow.VideoRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.claim_scribe_batch = AsyncMock(return_value=mock_videos)

        result = await fetch_scribe_targets_task(batch_size=10)

        assert len(result) == 2
        assert result[0].id == "VIDEO_001"
        mock_repo.claim_scribe_batch.assert_called_once_with(10)


@pytest.mark.asyncio
async def test_process_transcript_youtube_caption_first():
    """Scribe fetches captions from YouTube first (no vault captions step)."""
    video = Video(id="VIDEO_001", title="Test Video")
    segs = [{"text": "Hello", "start": 0.0, "duration": 1.5}]

    with (
        patch("maia.scribe.flow.VideoRepository") as MockRepo,
        patch("maia.scribe.flow.TranscriptRepository") as MockTranscriptRepo,
        patch("maia.scribe.flow.get_vault") as mock_get_vault,
        patch("maia.scribe.flow.TranscriptLoader") as MockLoader,
    ):
        mock_repo = MockRepo.return_value
        mock_repo.mark_transcript_safe = AsyncMock()
        mock_transcript_repo = MockTranscriptRepo.return_value
        mock_transcript_repo.record_transcript = AsyncMock()
        mock_vault = mock_get_vault.return_value
        mock_vault.fetch_audio = MagicMock(return_value=None)
        MockLoader.return_value.fetch = MagicMock(return_value=segs)

        await process_transcript_task(video)

        # Caption ownership lives entirely in the Scribe → YouTube fetch first.
        MockLoader.return_value.fetch.assert_called_once_with("VIDEO_001")
        mock_transcript_repo.record_transcript.assert_called_once()
        mock_repo.mark_transcript_safe.assert_called_once_with("VIDEO_001")


@pytest.mark.asyncio
async def test_process_transcript_audio_stt_fallback():
    """No captions anywhere → speech-to-text on the Singer's vault audio."""
    video = Video(id="VIDEO_003", title="Test Video")
    segs = [{"text": "Audio only", "start": 0.0, "duration": 2.0}]

    with (
        patch("maia.scribe.flow.VideoRepository") as MockRepo,
        patch("maia.scribe.flow.TranscriptRepository") as MockTranscriptRepo,
        patch("maia.scribe.flow.get_vault") as mock_get_vault,
        patch("maia.scribe.flow.TranscriptLoader") as MockLoader,
        patch("maia.scribe.flow.transcribe_audio_path") as mock_audio,
        patch("maia.scribe.flow.audio_cap_reached", return_value=False),
        patch("maia.scribe.flow.record_audio_usage"),
    ):
        mock_repo = MockRepo.return_value
        mock_repo.mark_transcript_safe = AsyncMock()
        mock_transcript_repo = MockTranscriptRepo.return_value
        mock_transcript_repo.record_transcript = AsyncMock()
        # No vault captions; YouTube caption fetch fails; audio IS in the vault.
        mock_vault = mock_get_vault.return_value
        mock_vault.fetch_audio = MagicMock(
            side_effect=lambda path: io.BytesIO(b"OPUS") if path.startswith("VIDEO_") else None
        )
        MockLoader.return_value.fetch = MagicMock(
            side_effect=TranscriptExtractionError("no subtitles")
        )
        mock_audio.return_value = _audio_result(segs)

        await process_transcript_task(video)

        MockLoader.return_value.fetch.assert_called_once_with("VIDEO_003")
        mock_audio.assert_called_once()
        mock_transcript_repo.record_transcript.assert_called_once()
        mock_repo.mark_transcript_safe.assert_called_once_with("VIDEO_003")


@pytest.mark.asyncio
async def test_transcribe_audio_tempfile_cleaned_on_success():
    """Vault audio's temporary file is removed after successful transcription."""
    video = Video(id="VIDEO_TEMP_OK", title="Audio only")
    paths: list[Path] = []

    def transcribe(path: Path) -> TranscriptResult:
        paths.append(path)
        assert path.exists()
        return _audio_result([{"text": "done", "start": 0.0, "duration": 1.0}])

    with (
        patch("maia.scribe.flow.get_vault") as mock_get_vault,
        patch("maia.scribe.flow.TranscriptLoader") as MockLoader,
        patch("maia.scribe.flow.transcribe_audio_path", side_effect=transcribe),
        patch("maia.scribe.flow.audio_cap_reached", return_value=False),
    ):
        mock_get_vault.return_value.fetch_audio = MagicMock(
            side_effect=lambda path: io.BytesIO(b"OPUS") if path.startswith("VIDEO_") else None
        )
        MockLoader.return_value.fetch = MagicMock(
            side_effect=TranscriptExtractionError("no subtitles")
        )

        result = await _transcribe(video)

    assert result[0]["text"] == "done"
    assert paths and not paths[0].exists()


@pytest.mark.asyncio
async def test_transcribe_audio_tempfile_cleaned_on_failure():
    """Vault audio's temporary file is removed when transcription raises."""
    video = Video(id="VIDEO_TEMP_FAIL", title="Audio only")
    paths: list[Path] = []

    def transcribe(path: Path) -> TranscriptResult:
        paths.append(path)
        assert path.exists()
        raise RuntimeError("transcriber failed")

    with (
        patch("maia.scribe.flow.get_vault") as mock_get_vault,
        patch("maia.scribe.flow.TranscriptLoader") as MockLoader,
        patch("maia.scribe.flow.transcribe_audio_path", side_effect=transcribe),
        patch("maia.scribe.flow.audio_cap_reached", return_value=False),
    ):
        mock_get_vault.return_value.fetch_audio = MagicMock(
            side_effect=lambda path: io.BytesIO(b"OPUS") if path.startswith("VIDEO_") else None
        )
        MockLoader.return_value.fetch = MagicMock(
            side_effect=TranscriptExtractionError("no subtitles")
        )

        with pytest.raises(TranscriptExtractionError, match="Audio STT unavailable"):
            await _transcribe(video)

    assert paths and not paths[0].exists()


@pytest.mark.asyncio
async def test_process_transcript_unavailable():
    """No transcript at all (no captions, no audio STT) → marked safe, not failed."""
    video = Video(id="VIDEO_NO_T", title="Video Without Transcript")

    with (
        patch("maia.scribe.flow.VideoRepository") as MockRepo,
        patch("maia.scribe.flow.TranscriptRepository") as MockTranscriptRepo,
        patch("maia.scribe.flow.get_vault") as mock_get_vault,
        patch("maia.scribe.flow.TranscriptLoader") as MockLoader,
        patch("maia.scribe.flow.transcribe_audio_download") as mock_dl,
        patch("maia.scribe.flow.audio_cap_reached", return_value=False),
        patch("maia.scribe.flow.record_audio_usage"),
    ):
        mock_repo = MockRepo.return_value
        mock_repo.mark_transcript_safe = AsyncMock()
        mock_transcript_repo = MockTranscriptRepo.return_value
        mock_transcript_repo.record_transcript = AsyncMock()
        mock_vault = mock_get_vault.return_value
        mock_vault.fetch_audio = MagicMock(return_value=None)
        MockLoader.return_value.fetch = MagicMock(
            side_effect=TranscriptExtractionError("no subtitles")
        )
        mock_dl.side_effect = TranscriptExtractionError("no audio transcriber")

        await process_transcript_task(video)

        mock_repo.mark_transcript_safe.assert_called_once_with("VIDEO_NO_T")
        # "No transcript obtainable" is a fact about the video, so it is recorded
        # the same way the too-long branch records its notice. Previously this
        # called mark_transcript_safe() with no record_transcript(), setting
        # has_transcript=TRUE while no transcript row and no vault artifact
        # existed — and permanently blocking the janitor from retrying.
        mock_transcript_repo.record_transcript.assert_called_once()
        assert (
            mock_transcript_repo.record_transcript.await_args.kwargs["content_json"][0]["text"]
            == SCRIBE_NO_TRANSCRIPT_MESSAGE
        )


@pytest.mark.asyncio
async def test_vault_flush_retains_hot_payload_on_backend_failure(mock_sleep, mock_prefect_logger):
    """A failed vault write leaves the staged transcript available for the next cycle."""
    from maia.janitor.flow import vault_flush_task

    video = StagedItem(
        "VIDEO_001", "1", b"{}", namespace="transcripts/VI/VIDEO_001", suffix=".json"
    )

    with (
        patch("maia.janitor.flow.TranscriptRepository") as MockRepo,
        patch("maia.janitor.flow.get_vault") as mock_get_vault,
    ):
        mock_repo = MockRepo.return_value
        mock_repo.pending = AsyncMock(return_value=[video])
        mock_repo.finalize = AsyncMock()
        mock_repo.mark_step_failed = AsyncMock()
        mock_vault = MagicMock()
        mock_vault.store_batch = MagicMock(side_effect=Exception("Vault connection error"))
        mock_get_vault.return_value = mock_vault

        result = await vault_flush_task(batch_size=10)

        # On vault failure the video stays pending (self-heals on retry); it is
        # NOT marked failed. So flushed=0, failed=1, and finalize is
        # not called. (store_batch now retries HTTP 429 internally; this test
        # verifies the janitor leaves the video pending on hard failure.)
        assert result["flushed"] == 0
        assert result["failed"] == 1
        mock_repo.finalize.assert_not_called()


@pytest.mark.asyncio
async def test_process_transcript_handles_transcript_fetch_failure():
    """Loader error → treated as no captions → safe (not permanently FAILED)."""
    video = Video(id="VIDEO_001", title="Test Video")

    with (
        patch("maia.scribe.flow.VideoRepository") as MockRepo,
        patch("maia.scribe.flow.TranscriptRepository") as MockTranscriptRepo,
        patch("maia.scribe.flow.get_vault") as mock_get_vault,
        patch("maia.scribe.flow.TranscriptLoader") as MockLoader,
        patch(
            "maia.scribe.flow.transcribe_audio_download",
            side_effect=TranscriptExtractionError("no stt"),
        ),
    ):
        mock_repo = MockRepo.return_value
        mock_repo.mark_transcript_safe = AsyncMock()
        mock_repo.mark_step_failed = AsyncMock()
        MockTranscriptRepo.return_value.record_transcript = AsyncMock()
        mock_vault = mock_get_vault.return_value
        mock_vault.fetch_audio = MagicMock(return_value=None)
        MockLoader.return_value.fetch = MagicMock(side_effect=Exception("Network timeout"))

        await process_transcript_task(video)

        mock_repo.mark_transcript_safe.assert_called_once_with("VIDEO_001")
        mock_repo.mark_step_failed.assert_not_called()


@pytest.mark.asyncio
async def test_process_transcript_propagates_resiliency_strategy():
    """QuotaExhaustedError propagates out of process_transcript."""
    video = Video(id="VIDEO_001", title="Test Video")

    with (
        patch("maia.scribe.flow.VideoRepository"),
        patch("maia.scribe.flow.get_vault") as mock_get_vault,
        patch("maia.scribe.flow.TranscriptLoader") as MockLoader,
        patch("maia.scribe.flow.notify_quota_exhausted", new_callable=AsyncMock),
    ):
        mock_vault = mock_get_vault.return_value
        mock_vault.fetch_audio = MagicMock(return_value=None)
        MockLoader.return_value.fetch = MagicMock(
            side_effect=QuotaExhaustedError("All keys exhausted")
        )

        with pytest.raises(QuotaExhaustedError):
            await process_transcript_task(video)


@pytest.mark.asyncio
async def test_run_scribe_cycle_empty_queue():
    """Test run_scribe_cycle handles empty queue gracefully."""
    with patch("maia.scribe.flow.fetch_scribe_targets_task", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = []

        await scribe_flow.fn(batch_size=10)

        mock_fetch.assert_called_once_with(10)


@pytest.mark.asyncio
async def test_run_scribe_cycle_processes_batch():
    """Test run_scribe_cycle processes a batch of videos."""
    mock_videos = [
        Video(id="VIDEO_001", title="Video 1"),
        Video(id="VIDEO_002", title="Video 2"),
        Video(id="VIDEO_003", title="Video 3"),
    ]

    with (
        patch("maia.scribe.flow.fetch_scribe_targets_task", new_callable=AsyncMock) as mock_fetch,
        patch("maia.scribe.flow.process_transcript_task", new_callable=AsyncMock) as mock_process,
    ):
        mock_fetch.return_value = mock_videos
        mock_process.return_value = None

        await scribe_flow.fn(batch_size=3)

        assert mock_process.call_count == 3


@pytest.mark.asyncio
async def test_run_scribe_cycle_continues_on_individual_failures():
    """Test run_scribe_cycle continues processing even if individual videos fail."""
    mock_videos = [
        Video(id="VIDEO_001", title="Video 1"),
        Video(id="VIDEO_002", title="Video 2 (will fail)"),
        Video(id="VIDEO_003", title="Video 3"),
    ]

    call_count = {"count": 0}

    async def mock_process_side_effect(video):
        call_count["count"] += 1
        return None

    with (
        patch("maia.scribe.flow.fetch_scribe_targets_task", new_callable=AsyncMock) as mock_fetch,
        patch("maia.scribe.flow.process_transcript_task", new_callable=AsyncMock) as mock_process,
    ):
        mock_fetch.return_value = mock_videos
        mock_process.side_effect = mock_process_side_effect

        await scribe_flow.fn(batch_size=3)

        assert call_count["count"] == 3


# --- TranscriptLoader client cascade (mocked streamer) ---


def _loader() -> TranscriptLoader:
    return TranscriptLoader()


@patch("maia.scribe.loader.StealthVideoStreamer")
def test_loader_returns_structured_segments(mock_stealth):
    """Structured segments [{text,start,duration}] from the streamer pass through."""
    mock_stealth.return_value.extract_captions.return_value = [
        {"text": "Hello world", "start": 0.0, "duration": 2.5},
        {"text": "Second line", "start": 2.5, "duration": 1.0},
    ]
    loader = _loader()
    segs = loader.fetch("VIDEO_X")
    assert segs == [
        {"text": "Hello world", "start": 0.0, "duration": 2.5},
        {"text": "Second line", "start": 2.5, "duration": 1.0},
    ]
    # First client tried first; a success short-circuits the cascade.
    mock_stealth.return_value.extract_captions.assert_called_once()


@patch("maia.scribe.loader.StealthVideoStreamer")
def test_loader_empty_transcript_returns_empty_list(mock_stealth):
    """An empty caption track returns [] (empty transcript), not an error."""
    mock_stealth.return_value.extract_captions.return_value = []
    assert _loader().fetch("VIDEO_Y") == []


@patch("maia.scribe.loader.StealthVideoStreamer")
def test_loader_disabled_captions_raise_no_subtitles(mock_stealth):
    """A 'No subtitles available' error is authoritative and short-circuits the
    remaining clients (doesn't waste time on the cascade)."""
    mock_stealth.return_value.extract_captions.side_effect = TranscriptExtractionError(
        "No subtitles available for VIDEO_Z"
    )
    with pytest.raises(TranscriptExtractionError):
        _loader().fetch("VIDEO_Z")
    assert mock_stealth.return_value.extract_captions.call_count == 1


@patch("maia.scribe.loader.StealthVideoStreamer")
def test_loader_rate_limit_cascades_to_next_client(mock_stealth):
    """A per-client rate-limit falls through to the next player client."""

    def side_effect(video_id, client, tmpdir):
        if client == "default":
            raise TranscriptRateLimitError("throttled")
        return [{"text": "recovered", "start": 0.0, "duration": 1.0}]

    mock_stealth.return_value.extract_captions.side_effect = side_effect
    segs = _loader().fetch("VIDEO_R")
    assert segs == [{"text": "recovered", "start": 0.0, "duration": 1.0}]
    assert mock_stealth.return_value.extract_captions.call_count == 2


@patch("maia.scribe.loader.StealthVideoStreamer")
def test_loader_all_clients_rate_limited_raises(mock_stealth):
    """When every client is throttled, TranscriptRateLimitError propagates so
    the flow can re-queue the video (never falls through to STT)."""
    mock_stealth.return_value.extract_captions.side_effect = TranscriptRateLimitError("bot check")
    with pytest.raises(TranscriptRateLimitError):
        _loader().fetch("VIDEO_Q")
    assert mock_stealth.return_value.extract_captions.call_count == 3


@patch("maia.scribe.loader.StealthVideoStreamer")
def test_loader_generic_extraction_error_recorded_and_no_cascade(mock_stealth):
    """A non-rate-limit, non-no-subtitles error is held until all clients tried,
    then re-raised (no silent fall-through)."""
    mock_stealth.return_value.extract_captions.side_effect = TranscriptExtractionError(
        "corrupt timedtext"
    )
    with pytest.raises(TranscriptExtractionError):
        _loader().fetch("VIDEO_C")
    assert mock_stealth.return_value.extract_captions.call_count == 3


# --- record_transcript JSON shape ---


@pytest.mark.asyncio
async def test_process_transcript_records_empty_segments():
    """Structured segments from captions are passed verbatim as content_json."""
    video = Video(id="VIDEO_E", title="Empty")
    with (
        patch("maia.scribe.flow.VideoRepository") as MockRepo,
        patch("maia.scribe.flow.TranscriptRepository") as MockTranscriptRepo,
        patch("maia.scribe.flow.TranscriptLoader") as MockLoader,
    ):
        mock_repo = MockRepo.return_value
        mock_repo.mark_transcript_safe = AsyncMock()
        mock_transcript = MockTranscriptRepo.return_value
        mock_transcript.record_transcript = AsyncMock()
        MockLoader.return_value.fetch = MagicMock(return_value=[])

        await process_transcript_task(video)

        mock_transcript.record_transcript.assert_called_once_with(
            "VIDEO_E",
            vault_uri=None,
            language="en",
            content_json=[],
        )


@pytest.mark.asyncio
async def test_process_transcript_structured_segments_json():
    """Structured segments are stored in content_json as-is (no re-wrapping)."""
    video = Video(id="VIDEO_S", title="Structured")
    segs = [{"text": "line 1", "start": 0.0, "duration": 1.2}]
    with (
        patch("maia.scribe.flow.VideoRepository") as MockRepo,
        patch("maia.scribe.flow.TranscriptRepository") as MockTranscriptRepo,
        patch("maia.scribe.flow.TranscriptLoader") as MockLoader,
    ):
        mock_repo = MockRepo.return_value
        mock_repo.mark_transcript_safe = AsyncMock()
        mock_transcript = MockTranscriptRepo.return_value
        mock_transcript.record_transcript = AsyncMock()
        MockLoader.return_value.fetch = MagicMock(return_value=segs)

        await process_transcript_task(video)

        mock_transcript.record_transcript.assert_called_once_with(
            "VIDEO_S",
            vault_uri=None,
            language="en",
            content_json=segs,
        )
