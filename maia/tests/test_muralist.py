"""
Tests for Maia Muralist module (full-video archival consumer).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from atlas.utils import QuotaExhaustedError
from maia.media.streamer import VideoExtractionError
from maia.muralist.flow import (
    fetch_muralist_targets_task,
    muralist_flow,
    process_video_task,
)

FAKE_VIDEO = b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 200


@pytest.mark.asyncio
async def test_fetch_muralist_targets_empty():
    with patch("maia.muralist.flow.VideoRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.claim_muralist_batch = AsyncMock(return_value=[])

        result = await fetch_muralist_targets_task.fn(batch_size=5)

        assert result == []
        mock_repo.claim_muralist_batch.assert_called_once_with(5)


@pytest.mark.asyncio
async def test_fetch_muralist_targets_with_videos():
    from atlas.models import Video

    mock_videos = [
        Video(id="VIDEO_001", title="Test Video 1"),
        Video(id="VIDEO_002", title="Test Video 2"),
    ]

    with patch("maia.muralist.flow.VideoRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.claim_muralist_batch = AsyncMock(return_value=mock_videos)

        result = await fetch_muralist_targets_task.fn(batch_size=5)

        assert len(result) == 2
        assert result[0].id == "VIDEO_001"
        mock_repo.claim_muralist_batch.assert_called_once_with(5)


@pytest.mark.asyncio
async def test_process_video_successful():
    """process_video downloads + returns (vid, bytes, ext); store deferred."""
    from atlas.models import Video

    video = Video(id="VIDEO_001", title="Test Video")

    fake_path = MagicMock()
    fake_path.read_bytes = MagicMock(return_value=FAKE_VIDEO)
    fake_path.suffix = ".mp4"

    with (
        patch("maia.muralist.flow.VideoRepository") as MockRepo,
        patch("maia.muralist.flow.StealthVideoStreamer") as MockStreamer,
    ):
        mock_repo = MockRepo.return_value
        mock_repo.mark_video_safe = AsyncMock()
        mock_repo.mark_step_failed = AsyncMock()
        mock_repo.release_clip_to_pending = AsyncMock()
        mock_streamer_instance = MockStreamer.return_value
        mock_streamer_instance.extract_video = MagicMock(return_value=fake_path)

        result = await process_video_task.fn(video)

        assert isinstance(result, tuple)
        assert result[0] == "VIDEO_001"
        assert result[1] == FAKE_VIDEO
        assert result[2] == "mp4"
        mock_repo.mark_video_safe.assert_not_called()
        mock_repo.mark_step_failed.assert_not_called()
        mock_repo.release_clip_to_pending.assert_not_called()


@pytest.mark.asyncio
async def test_process_video_handles_extraction_failure():
    """VideoExtractionError → released to PENDING (transient), returns None."""
    from atlas.models import Video

    video = Video(id="VIDEO_001", title="Test Video")

    with (
        patch("maia.muralist.flow.VideoRepository") as MockRepo,
        patch("maia.muralist.flow.StealthVideoStreamer") as MockStreamer,
    ):
        mock_repo = MockRepo.return_value
        mock_repo.release_clip_to_pending = AsyncMock()
        mock_repo.mark_step_failed = AsyncMock()
        mock_streamer_instance = MockStreamer.return_value
        mock_streamer_instance.extract_video = MagicMock(
            side_effect=VideoExtractionError("yt-dlp failed")
        )

        result = await process_video_task.fn(video)

        assert result is None
        mock_repo.release_clip_to_pending.assert_called_once_with("VIDEO_001")
        mock_repo.mark_step_failed.assert_not_called()


@pytest.mark.asyncio
async def test_process_video_propagates_quota():
    from atlas.models import Video

    video = Video(id="VIDEO_001", title="Test Video")

    with (
        patch("maia.muralist.flow.VideoRepository"),
        patch("maia.muralist.flow.StealthVideoStreamer") as MockStreamer,
        patch("maia.muralist.flow.notify_quota_exhausted", new_callable=AsyncMock),
    ):
        mock_streamer_instance = MockStreamer.return_value
        mock_streamer_instance.extract_video = MagicMock(
            side_effect=QuotaExhaustedError("All keys exhausted")
        )

        with pytest.raises(QuotaExhaustedError):
            await process_video_task.fn(video)


@pytest.mark.asyncio
async def test_run_muralist_cycle_batched_store():
    """Batched clips stored in ONE vault commit; videos marked video-safe."""
    from atlas.models import Video

    mock_videos = [Video(id="VIDEO_001", title="T1"), Video(id="VIDEO_002", title="T2")]

    with (
        patch(
            "maia.muralist.flow.fetch_muralist_targets_task", new_callable=AsyncMock
        ) as mock_fetch,
        patch(
            "maia.muralist.flow.process_video_task",
            new_callable=AsyncMock,
            side_effect=[
                ("VIDEO_001", FAKE_VIDEO, "mp4"),
                ("VIDEO_002", FAKE_VIDEO, "mp4"),
            ],
        ),
        patch("maia.muralist.flow.VideoRepository") as MockRepo,
        patch("maia.muralist.flow.get_vault") as mock_get_vault,
        patch("maia.muralist.flow.vault_op_with_retry", new_callable=AsyncMock) as mock_vault_retry,
        patch("maia.base.clear_quota_exhausted"),
    ):
        mock_fetch.return_value = mock_videos
        mock_repo = MockRepo.return_value
        mock_repo.mark_video_safe = AsyncMock()
        mock_repo.mark_step_failed = AsyncMock()
        mock_vault = mock_get_vault.return_value
        mock_vault.store_batch = MagicMock()

        await muralist_flow.fn(batch_size=2)

        mock_vault_retry.assert_called_once()
        assert mock_repo.mark_video_safe.call_count == 2
        mock_repo.mark_step_failed.assert_not_called()


@pytest.mark.asyncio
async def test_run_muralist_cycle_vault_failure_marks_failed():
    from atlas.models import Video

    mock_videos = [Video(id="VIDEO_001", title="T1")]

    with (
        patch(
            "maia.muralist.flow.fetch_muralist_targets_task", new_callable=AsyncMock
        ) as mock_fetch,
        patch(
            "maia.muralist.flow.process_video_task",
            new_callable=AsyncMock,
            return_value=("VIDEO_001", FAKE_VIDEO, "mp4"),
        ),
        patch("maia.muralist.flow.VideoRepository") as MockRepo,
        patch("maia.muralist.flow.get_vault") as mock_get_vault,
        patch(
            "maia.muralist.flow.vault_op_with_retry",
            new_callable=AsyncMock,
            side_effect=Exception("Vault error"),
        ),
        patch("maia.base.clear_quota_exhausted"),
    ):
        mock_fetch.return_value = mock_videos
        mock_repo = MockRepo.return_value
        mock_repo.mark_video_safe = AsyncMock()
        mock_repo.mark_step_failed = AsyncMock()
        mock_vault = mock_get_vault.return_value
        mock_vault.store_batch = MagicMock()

        await muralist_flow.fn(batch_size=1)

        mock_repo.mark_step_failed.assert_called_once_with("VIDEO_001", "clip")
        mock_repo.mark_video_safe.assert_not_called()
