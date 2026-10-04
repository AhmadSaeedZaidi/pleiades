"""
Tests for Maia Singer module (local audio extractor + vault storer; no STT).
"""

import subprocess
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from atlas.models import Video
from maia.media.streamer import AudioExtractionError
from maia.singer.flow import (
    SingerAgent,
    fetch_singer_targets_task,
    singer_flow,
    store_audio_task,
)

FAKE_OPUS = b"OggS" + b"\x00" * 200
FAKE_RAW = b"RAWDATA" + b"\x00" * 100


def _fake_materialize(_path, destination):
    destination.write_bytes(FAKE_RAW)
    return True


def _fake_extract(src, dst):
    """Stand-in for ffmpeg: write FAKE_OPUS to the destination path."""
    dst.write_bytes(FAKE_OPUS)
    return dst


@pytest.mark.asyncio
async def test_fetch_singer_targets_empty():
    with patch("maia.singer.flow.VideoRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.claim_singer_batch = AsyncMock(return_value=[])

        result = await fetch_singer_targets_task(batch_size=10)

        assert result == []
        mock_repo.claim_singer_batch.assert_called_once_with(10)


@pytest.mark.asyncio
async def test_fetch_singer_targets_with_videos():
    mock_videos = [
        Video(id="VIDEO_001", title="Test Video 1", raw_uri="raw/VIDEO_001.webm"),
        Video(id="VIDEO_002", title="Test Video 2", raw_uri="raw/VIDEO_002.webm"),
    ]

    with patch("maia.singer.flow.VideoRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.claim_singer_batch = AsyncMock(return_value=mock_videos)

        result = await fetch_singer_targets_task(batch_size=10)

        assert len(result) == 2
        assert result[0].id == "VIDEO_001"
        mock_repo.claim_singer_batch.assert_called_once_with(10)


@pytest.mark.asyncio
async def test_store_audio_successful():
    """Raw artifact is fetched, ffmpeg-extracted, staged, marked safe."""
    video = Video(id="VIDEO_001", title="Test Video", raw_uri="raw/VIDEO_001.webm")

    with (
        patch("maia.singer.flow.VideoRepository") as MockRepo,
        patch("maia.singer.flow.get_vault") as mock_get_vault,
        patch("maia.singer.flow.extract_audio_ffmpeg", side_effect=_fake_extract),
    ):
        mock_repo = MockRepo.return_value
        mock_repo.mark_audio_safe = AsyncMock()
        mock_repo.mark_step_failed = AsyncMock()
        mock_vault = mock_get_vault.return_value
        mock_vault.fetch_binary_to_path = MagicMock(side_effect=_fake_materialize)

        result = await store_audio_task(video)

        assert result is not None
        assert len(result) == 1
        vid_id, rel, audio_bytes = result[0]
        assert vid_id == "VIDEO_001"
        assert rel == "media/audio/VI/VIDEO_001.opus"
        assert audio_bytes == FAKE_OPUS
        mock_vault.fetch_binary_to_path.assert_called_once()
        mock_repo.mark_audio_safe.assert_not_called()
        mock_repo.mark_step_failed.assert_not_called()


@pytest.mark.asyncio
async def test_store_audio_missing_raw_marks_failed():
    """Raw artifact absent in vault though fetched=TRUE → mark FAILED."""
    video = Video(id="VIDEO_001", title="Test Video", raw_uri="raw/VIDEO_001.webm")

    with (
        patch("maia.singer.flow.VideoRepository") as MockRepo,
        patch("maia.singer.flow.get_vault") as mock_get_vault,
    ):
        mock_repo = MockRepo.return_value
        mock_repo.mark_step_failed = AsyncMock()
        mock_vault = mock_get_vault.return_value
        mock_vault.fetch_binary_to_path = MagicMock(return_value=False)

        result = await store_audio_task(video)

        assert result is None
        mock_repo.mark_step_failed.assert_called_once_with("VIDEO_001", "audio")


@pytest.mark.asyncio
async def test_store_audio_no_raw_uri_marks_failed():
    """Video flagged fetched but raw_uri is null → mark FAILED."""
    video = Video(id="VIDEO_001", title="Test Video", raw_uri=None)

    with patch("maia.singer.flow.VideoRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.mark_step_failed = AsyncMock()

        result = await store_audio_task(video)

        assert result is None
        mock_repo.mark_step_failed.assert_called_once_with("VIDEO_001", "audio")


@pytest.mark.asyncio
async def test_store_audio_extraction_failure_marks_failed():
    """ffmpeg extraction error → marked FAILED (not released)."""
    video = Video(id="VIDEO_001", title="Test Video", raw_uri="raw/VIDEO_001.webm")

    with (
        patch("maia.singer.flow.VideoRepository") as MockRepo,
        patch("maia.singer.flow.get_vault") as mock_get_vault,
        patch(
            "maia.singer.flow.extract_audio_ffmpeg",
            side_effect=AudioExtractionError("ffmpeg died"),
        ),
    ):
        mock_repo = MockRepo.return_value
        mock_repo.mark_step_failed = AsyncMock()
        mock_vault = mock_get_vault.return_value
        mock_vault.fetch_binary_to_path = MagicMock(side_effect=_fake_materialize)

        result = await store_audio_task(video)

        assert result is None
        mock_repo.mark_step_failed.assert_called_once_with("VIDEO_001", "audio")


@pytest.mark.asyncio
async def test_store_audio_timeout_retries_only_audio_step():
    """A transient ffmpeg timeout must preserve the already-fetched raw step."""
    video = Video(id="VIDEO_001", title="Test Video", raw_uri="raw/VIDEO_001.webm")

    with (
        patch("maia.singer.flow.VideoRepository") as MockRepo,
        patch("maia.singer.flow.get_vault") as mock_get_vault,
        patch(
            "maia.singer.flow.extract_audio_ffmpeg",
            side_effect=subprocess.TimeoutExpired("ffmpeg", 900),
        ),
    ):
        mock_repo = MockRepo.return_value
        mock_repo.release_audio_to_pending = AsyncMock()
        mock_repo.release_raw_to_pending = AsyncMock()
        mock_vault = mock_get_vault.return_value
        mock_vault.fetch_binary_to_path = MagicMock(side_effect=_fake_materialize)

        result = await store_audio_task(video)

        assert result is None
        mock_repo.release_audio_to_pending.assert_awaited_once_with("VIDEO_001")
        mock_repo.release_raw_to_pending.assert_not_awaited()


@pytest.mark.asyncio
async def test_store_audio_read_failure_releases_audio_step():
    """An unreadable FFmpeg output must not strand audio in PROCESSING."""
    video = Video(id="VIDEO_001", title="Test Video", raw_uri="raw/VIDEO_001.webm")

    with (
        patch("maia.singer.flow.VideoRepository") as MockRepo,
        patch("maia.singer.flow.get_vault") as mock_get_vault,
        patch("maia.singer.flow.extract_audio_ffmpeg", side_effect=_fake_extract),
        patch("maia.singer.flow.Path.read_bytes", side_effect=OSError("read failed")),
    ):
        mock_repo = MockRepo.return_value
        mock_repo.release_audio_to_pending = AsyncMock()
        mock_vault = mock_get_vault.return_value
        mock_vault.fetch_binary_to_path = MagicMock(side_effect=_fake_materialize)

        result = await store_audio_task(video)

    assert result is None
    mock_repo.release_audio_to_pending.assert_awaited_once_with("VIDEO_001")


@pytest.mark.asyncio
async def test_run_singer_cycle_empty_queue():
    with patch("maia.singer.flow.fetch_singer_targets_task", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = []

        await singer_flow.fn(batch_size=10)

        mock_fetch.assert_called_once_with(10)


@pytest.mark.asyncio
async def test_run_singer_cycle_processes_batch():
    mock_videos = [
        Video(id="VIDEO_001", title="T1", raw_uri="raw/VIDEO_001.webm"),
        Video(id="VIDEO_002", title="T2", raw_uri="raw/VIDEO_002.webm"),
    ]

    with (
        patch("maia.singer.flow.fetch_singer_targets_task", new_callable=AsyncMock) as mock_fetch,
        patch(
            "maia.singer.flow.store_audio_task",
            new_callable=AsyncMock,
            side_effect=[
                [("VIDEO_001", "audio/VIDEO_001.opus", FAKE_OPUS)],
                [("VIDEO_002", "audio/VIDEO_002.opus", FAKE_OPUS)],
            ],
        ),
        patch("maia.singer.flow.VideoRepository") as MockRepo,
        patch("maia.singer.flow.get_vault") as mock_get_vault,
        patch("maia.singer.flow.vault_op_with_retry", new_callable=AsyncMock),
    ):
        mock_fetch.return_value = mock_videos
        mock_repo = MockRepo.return_value
        mock_repo.mark_audio_safe = AsyncMock()
        mock_repo.reclaim_raw_if_complete = AsyncMock()
        mock_vault = mock_get_vault.return_value
        mock_vault.delete_files = MagicMock()

        await singer_flow.fn(batch_size=2)

        # Raw is reclaimed only once both audio + visuals are done, via the
        # repository (which shells out to the vault) — not the singer directly.
        assert mock_repo.mark_audio_safe.call_count == 2
        assert mock_repo.reclaim_raw_if_complete.call_count == 2
        mock_repo.reclaim_raw_if_complete.assert_any_call("VIDEO_001")
        mock_repo.reclaim_raw_if_complete.assert_any_call("VIDEO_002")
        mock_vault.delete_files.assert_not_called()


@pytest.mark.asyncio
async def test_singer_commits_incrementally_at_bounded_video_groups():
    """The fifth extraction starts only after the first four are persisted."""
    videos = [
        Video(id=f"VIDEO_{index:03d}", title="T", raw_uri=f"raw/{index}.webm") for index in range(6)
    ]
    trace: list[str] = []

    async def process(video: Video):
        trace.append(f"process:{video.id}")
        return [(video.id, f"audio/{video.id}.opus", FAKE_OPUS)]

    async def store(results):
        trace.append(f"store:{len(results)}")

    agent = SingerAgent()
    agent.throttle_seconds = 0
    agent.claim_batch = AsyncMock(return_value=videos)
    agent.process_one = AsyncMock(side_effect=process)
    agent.store_results = AsyncMock(side_effect=store)

    with patch("maia.singer.flow.clear_quota_exhausted"):
        result = await agent.run(batch_size=6)

    assert result == {"videos_processed": 6}
    assert trace == [
        "process:VIDEO_000",
        "process:VIDEO_001",
        "process:VIDEO_002",
        "process:VIDEO_003",
        "store:4",
        "process:VIDEO_004",
        "process:VIDEO_005",
        "store:2",
    ]


@pytest.mark.asyncio
async def test_singer_vault_failure_releases_only_audio_step():
    """A cold-store failure must not reset raw_phase or force a redownload."""
    agent = SingerAgent()
    extracted = [[("VIDEO_001", "audio/VIDEO_001.opus", FAKE_OPUS)]]

    with (
        patch("maia.singer.flow.VideoRepository") as MockRepo,
        patch("maia.singer.flow.get_vault"),
        patch(
            "maia.singer.flow.vault_op_with_retry",
            new_callable=AsyncMock,
            side_effect=RuntimeError("vault unavailable"),
        ),
    ):
        mock_repo = MockRepo.return_value
        mock_repo.release_audio_to_pending = AsyncMock()
        mock_repo.release_raw_to_pending = AsyncMock()
        mock_repo.reclaim_raw_if_complete = AsyncMock()

        await agent.store_results(extracted)

        mock_repo.release_audio_to_pending.assert_awaited_once_with("VIDEO_001")
        mock_repo.release_raw_to_pending.assert_not_awaited()
