"""
Integration tests for Maia Scribe.

These tests verify end-to-end behavior of Scribe transcription flows.
Mark as integration tests: pytest -m integration

Real Integration Testing: Uses real YouTube video (Blender Tutorial) with real captions.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from maia.scribe.flow import run_scribe_cycle


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scribe_complete_cycle(video_repo, channel_repo):
    """Test complete Scribe cycle using real Blender tutorial video with real transcripts."""
    test_video = {
        "id": {"videoId": "B0J27sf9N1Y"},
        "snippet": {
            "channelId": "UCOKHwx1VCdgnxwbjyb9Iu1g",
            "channelTitle": "Blender Guru",
            "title": "Beginner Blender 4.0 Tutorial (2023)",
            "publishedAt": "2023-11-16T00:00:00Z",
            "tags": ["blender", "tutorial"],
            "categoryId": "27",
            "defaultLanguage": "en",
        },
    }

    await video_repo.ingest_video_metadata(test_video)

    ch = await channel_repo.get_by_id("UCOKHwx1VCdgnxwbjyb9Iu1g")
    assert ch is not None, "Blender Guru channel should be indexed alongside the video"
    assert (ch.title or "").find("Blender") >= 0 or "Guru" in (ch.title or "")

    try:
        await run_scribe_cycle(batch_size=1)

        video = await video_repo._fetch_one("SELECT * FROM videos WHERE id = %s", ("B0J27sf9N1Y",))
        assert video["has_transcript"] is True, (
            f"Video should have transcript after scribe cycle, got {video['has_transcript']}"
        )

    except Exception as e:
        if "429" in str(e) or "HTTP Error 429" in str(e):
            pytest.fail(f"YouTube rate limit (429) encountered: {e}")
        elif "TranscriptsDisabled" in str(e) or "No transcript" in str(e):
            pytest.fail(f"Transcripts disabled or unavailable for this video: {e}")
        else:
            raise


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scribe_handles_unavailable_transcripts(video_repo):
    """Test Scribe handles videos with disabled transcripts."""
    video_data = {
        "id": {"videoId": "NO_TRANSCRIPT_001"},
        "snippet": {
            "channelId": "CHANNEL_001",
            "channelTitle": "Test Channel",
            "title": "Video without transcript",
            "publishedAt": datetime.now(UTC).isoformat(),
            "tags": ["test"],
            "categoryId": "28",
            "defaultLanguage": "en",
        },
    }

    await video_repo.ingest_video_metadata(video_data)

    with patch("maia.scribe.flow.TranscriptLoader") as MockLoader:
        mock_loader_instance = MagicMock()
        mock_loader_instance.fetch = MagicMock(return_value=None)
        MockLoader.return_value = mock_loader_instance

        await run_scribe_cycle(batch_size=1)

        video = await video_repo._fetch_one(
            "SELECT * FROM videos WHERE id = %s", ("NO_TRANSCRIPT_001",)
        )
        assert video["has_transcript"] is True, (
            "Video should be marked as transcribed even with unavailable transcripts"
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scribe_handles_resiliency_strategy(video_repo):
    """Test Scribe handles rate limit errors gracefully (marks video FAILED)."""
    video_data = {
        "id": {"videoId": "RATE_LIMIT_001"},
        "snippet": {
            "channelId": "CHANNEL_001",
            "channelTitle": "Test Channel",
            "title": "Video causing rate limit",
            "publishedAt": datetime.now(UTC).isoformat(),
            "tags": ["test"],
            "categoryId": "28",
            "defaultLanguage": "en",
        },
    }

    await video_repo.ingest_video_metadata(video_data)

    with patch("maia.scribe.flow.TranscriptLoader") as MockLoader:
        mock_loader_instance = MagicMock()
        mock_loader_instance.fetch = MagicMock(side_effect=RuntimeError("429 Rate Limit"))
        MockLoader.return_value = mock_loader_instance

        await run_scribe_cycle(batch_size=1)

        video = await video_repo._fetch_one(
            "SELECT * FROM videos WHERE id = %s", ("RATE_LIMIT_001",)
        )
        assert video["status"] == "FAILED", (
            f"Expected video status FAILED after rate limit error, got {video['status']}"
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scribe_empty_queue_returns_idle(video_repo):
    """Test Scribe handles empty queue gracefully."""
    await run_scribe_cycle(batch_size=10)

    # Empty queue must not leave any videos in a processed state.
    processed = await video_repo._fetch_all("SELECT * FROM videos WHERE status = 'PROCESSED'")
    assert len(processed) == 0, f"Empty queue should process 0 videos, got {len(processed)}"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scribe_batch_size_enforcement(video_repo):
    """Test Scribe respects batch size limit."""
    for i in range(10):
        video_data = {
            "id": {"videoId": f"BATCH_TEST_{i:03d}"},
            "snippet": {
                "channelId": "CHANNEL_001",
                "channelTitle": "Test Channel",
                "title": f"Video {i}",
                "publishedAt": datetime.now(UTC).isoformat(),
                "tags": ["test"],
                "categoryId": "28",
                "defaultLanguage": "en",
            },
        }
        await video_repo.ingest_video_metadata(video_data)

    mock_transcript = [{"text": "Test", "start": 0.0, "duration": 1.0}]

    with patch("maia.scribe.flow.TranscriptLoader") as MockLoader:
        mock_loader_instance = MagicMock()
        mock_loader_instance.fetch = MagicMock(return_value=mock_transcript)
        MockLoader.return_value = mock_loader_instance

        await run_scribe_cycle(batch_size=5)

        processed = await video_repo._fetch_all(
            "SELECT * FROM videos WHERE has_transcript = TRUE AND id LIKE 'BATCH_TEST_%%'"
        )
        assert len(processed) == 5, (
            f"Expected 5 videos processed with batch_size=5, got {len(processed)}"
        )

        remaining = await video_repo._fetch_all(
            "SELECT * FROM videos WHERE has_transcript = FALSE AND id LIKE 'BATCH_TEST_%%'"
        )
        assert len(remaining) == 5, f"Expected 5 unprocessed videos remaining, got {len(remaining)}"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scribe_sequential_processing(video_repo):
    """Test Scribe processes videos sequentially to manage rate limits."""

    for i in range(3):
        video_data = {
            "id": {"videoId": f"SEQ_TEST_{i:03d}"},
            "snippet": {
                "channelId": "CHANNEL_001",
                "channelTitle": "Test Channel",
                "title": f"Video {i}",
                "publishedAt": datetime.now(UTC).isoformat(),
                "tags": ["test"],
                "categoryId": "28",
                "defaultLanguage": "en",
            },
        }
        await video_repo.ingest_video_metadata(video_data)

    mock_transcript = [{"text": "Test", "start": 0.0, "duration": 1.0}]

    with patch("maia.scribe.flow.TranscriptLoader") as MockLoader:
        mock_loader_instance = MagicMock()
        mock_loader_instance.fetch = MagicMock(return_value=mock_transcript)
        MockLoader.return_value = mock_loader_instance

        await run_scribe_cycle(batch_size=3)

        processed = await video_repo._fetch_all(
            "SELECT * FROM videos WHERE has_transcript = TRUE AND id LIKE 'SEQ_TEST_%%'"
        )
        assert len(processed) == 3, (
            f"All 3 sequential videos should be processed, got {len(processed)}"
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scribe_vault_failure_marks_video_failed(video_repo, mock_sleep):
    """Test Scribe marks video as failed when vault storage fails after retries."""
    video_data = {
        "id": {"videoId": "VAULT_FAIL_001"},
        "snippet": {
            "channelId": "CHANNEL_001",
            "channelTitle": "Test Channel",
            "title": "Video with vault failure",
            "publishedAt": datetime.now(UTC).isoformat(),
            "tags": ["test"],
            "categoryId": "28",
            "defaultLanguage": "en",
        },
    }

    await video_repo.ingest_video_metadata(video_data)

    mock_transcript = [{"text": "Test", "start": 0.0, "duration": 1.0}]

    with (
        patch("maia.scribe.flow.TranscriptLoader") as MockLoader,
        patch("maia.scribe.flow.get_vault") as mock_get_vault,
    ):
        mock_loader_instance = MagicMock()
        mock_loader_instance.fetch = MagicMock(return_value=mock_transcript)
        MockLoader.return_value = mock_loader_instance

        mock_vault = MagicMock()
        mock_vault.store_transcript = MagicMock(side_effect=Exception("Vault connection error"))
        mock_get_vault.return_value = mock_vault

        await run_scribe_cycle(batch_size=1)

        video = await video_repo._fetch_one(
            "SELECT * FROM videos WHERE id = %s", ("VAULT_FAIL_001",)
        )
        assert video["status"] == "FAILED", (
            f"Expected video status FAILED after vault error, got {video['status']}"
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_scribe_retry_logic_on_network_errors(video_repo, mock_sleep):
    """Test Scribe retries transcript fetching on transient network errors."""
    video_data = {
        "id": {"videoId": "RETRY_TEST_001"},
        "snippet": {
            "channelId": "CHANNEL_001",
            "channelTitle": "Test Channel",
            "title": "Video with network issues",
            "publishedAt": datetime.now(UTC).isoformat(),
            "tags": ["test"],
            "categoryId": "28",
            "defaultLanguage": "en",
        },
    }

    await video_repo.ingest_video_metadata(video_data)

    mock_transcript = [{"text": "Success", "start": 0.0, "duration": 1.0}]

    with patch("maia.scribe.flow.TranscriptLoader") as MockLoader:
        mock_loader_instance = MagicMock()
        mock_loader_instance.fetch = MagicMock(
            side_effect=[
                ConnectionError("Network error"),
                ConnectionError("Network error"),
                mock_transcript,
            ]
        )
        MockLoader.return_value = mock_loader_instance

        await run_scribe_cycle(batch_size=1)

        assert mock_loader_instance.fetch.call_count == 3, (
            f"Expected 3 fetch calls (2 retries + 1 success),"
            f" got {mock_loader_instance.fetch.call_count}"
        )

        video = await video_repo._fetch_one(
            "SELECT * FROM videos WHERE id = %s", ("RETRY_TEST_001",)
        )
        assert video["has_transcript"] is True, (
            "Video should have transcript after successful retry"
        )


@pytest_asyncio.fixture
async def video_repo(fresh_db):
    """Provide VideoRepository for testing with real vault."""
    from atlas.repositories import VideoRepository

    yield VideoRepository()


@pytest_asyncio.fixture
async def channel_repo(fresh_db):
    """Provide ChannelRepository for testing with real vault."""
    from atlas.repositories import ChannelRepository

    yield ChannelRepository()
