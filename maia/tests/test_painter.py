"""
Tests for Maia Painter module.
"""

import io
import json
import subprocess
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from atlas.utils import QuotaExhaustedError
from maia.painter.flow import (
    fetch_painter_targets_task,
    painter_flow,
    process_frames_task,
)
from maia.painter.streamer import StealthVideoStreamer, StreamRateLimitError

# A RIFF/WEBP-magic buffer (frames are encoded as webp by default).
FAKE_WEBP = b"RIFF\x00\x00\x00\x00WEBP" + b"\x00" * 100


@pytest.fixture(autouse=True)
def painter_vault():
    """Legacy metadata fallback tests start with an empty, local fake vault."""
    with patch("maia.painter.flow.get_vault") as get_vault:
        get_vault.return_value.fetch_binary.return_value = None
        yield get_vault.return_value


def _fake_ffmpeg(bytes_out: bytes, returncode: int = 0):
    """Stand-in for subprocess.run that writes *bytes_out* to the output file.

    ``_ffmpeg_extract_frame`` encodes to a temp file (not stdout) and then reads
    it back, so the fake must materialise the bytes on disk at ``cmd[-1]``.
    """

    def _run(cmd, *args, **kwargs):
        out_path = cmd[-1]
        with open(out_path, "wb") as fh:
            fh.write(bytes_out)
        return subprocess.CompletedProcess(cmd, returncode, stdout=b"", stderr=b"")

    return _run


def _fake_subprocess(webp_bytes: bytes, video_stream: bool = True, duration: float = 200.0):
    """Fake for ``subprocess.run`` that handles BOTH ffprobe and ffmpeg.

    ``_has_video_stream`` and ``_ffprobe_duration`` shell out to ffprobe; frame
    extraction shells out to ffmpeg (which must materialise the webp on disk).
    """

    def _run(cmd, *args, **kwargs):
        if cmd[0] == "ffprobe":
            if "stream=index" in cmd:
                # _has_video_stream: report a video stream index if present.
                out = "0\n" if video_stream else ""
                return subprocess.CompletedProcess(cmd, 0, stdout=out, stderr="")
            # _ffprobe_duration: report the duration in seconds.
            return subprocess.CompletedProcess(cmd, 0, stdout=str(duration), stderr="")
        # ffmpeg frame extraction → write the fake webp to the output path.
        out_path = cmd[-1]
        with open(out_path, "wb") as fh:
            fh.write(webp_bytes)
        return subprocess.CompletedProcess(cmd, 0, stdout=b"", stderr=b"")

    return _run


async def test_fetch_painter_targets_empty():
    """Test fetch_painter_targets returns empty list when no videos need visual processing."""
    with patch("maia.painter.flow.VideoRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.claim_painter_batch = AsyncMock(return_value=[])

        result = await fetch_painter_targets_task(batch_size=5)

        assert result == []
        mock_repo.claim_painter_batch.assert_called_once_with(5)


@pytest.mark.asyncio
async def test_fetch_painter_targets_with_videos():
    """Test fetch_painter_targets returns videos needing visual processing."""
    from atlas.models import Video

    mock_videos = [
        Video(id="VIDEO_001", title="Test Video 1"),
        Video(id="VIDEO_002", title="Test Video 2"),
    ]

    with patch("maia.painter.flow.VideoRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.claim_painter_batch = AsyncMock(return_value=mock_videos)

        result = await fetch_painter_targets_task(batch_size=5)

        assert len(result) == 2
        assert result[0].id == "VIDEO_001"
        mock_repo.claim_painter_batch.assert_called_once_with(5)


def test_video_streamer_extract_heatmap_peaks():
    """Test StealthVideoStreamer extracts top N peaks from heatmap data."""
    streamer = StealthVideoStreamer()
    heatmap_data = [
        {"start_time": 10.0, "end_time": 11.0, "value": 0.5},
        {"start_time": 25.0, "end_time": 26.0, "value": 0.9},
        {"start_time": 50.0, "end_time": 51.0, "value": 0.3},
        {"start_time": 75.0, "end_time": 76.0, "value": 0.8},
        {"start_time": 100.0, "end_time": 101.0, "value": 0.7},
    ]
    peaks = streamer.extract_heatmap_peaks(heatmap_data, top_n=3)
    assert len(peaks) == 3
    assert peaks[0] == 25.0
    assert peaks[1] == 75.0
    assert peaks[2] == 100.0


def test_video_streamer_extract_heatmap_peaks_empty():
    """Test StealthVideoStreamer handles empty heatmap data."""
    streamer = StealthVideoStreamer()
    peaks = streamer.extract_heatmap_peaks([], top_n=5)
    assert peaks == []


@pytest.mark.asyncio
async def test_process_frames_successful_with_chapters():
    """Test process_frames successfully extracts frames using chapter strategy."""
    from atlas.models import Video

    video = Video(id="VIDEO_001", title="Test Video with Chapters")
    mock_video_info = {
        "url": "https://example.com/video.mp4",
        "duration": 200.0,
        "chapters": [
            {"start_time": 0.0, "title": "Intro"},
            {"start_time": 60.0, "title": "Main Content"},
            {"start_time": 120.0, "title": "Conclusion"},
        ],
        "heatmap": [],
    }
    fake_webp = FAKE_WEBP

    with (
        patch("maia.painter.flow.VideoRepository") as MockRepo,
        patch("maia.painter.flow.StealthVideoStreamer") as MockStreamer,
        patch("maia.painter.flow.subprocess.run", side_effect=_fake_ffmpeg(fake_webp)),
        patch("maia.painter.flow.get_vault") as mock_get_vault,
        patch("maia.painter.flow.vault_op_with_retry", new_callable=AsyncMock) as mock_vault_retry,
    ):
        mock_vault = mock_get_vault.return_value
        mock_repo = MockRepo.return_value
        mock_repo.mark_visuals_safe = AsyncMock()
        mock_repo.mark_step_failed = AsyncMock()

        mock_streamer_instance = MockStreamer.return_value
        mock_streamer_instance.extract_info = MagicMock(return_value=mock_video_info)

        mock_vault.store_visual_evidence = MagicMock()

        result = await process_frames_task(video)

        # Storage is deferred to the flow (batched into one commit).
        assert isinstance(result, tuple)
        assert result[0] == "VIDEO_001"
        assert len(result[1]) > 0
        mock_vault_retry.assert_not_called()
        mock_vault.store_visual_evidence.assert_not_called()
        mock_repo.mark_visuals_safe.assert_not_called()


@pytest.mark.asyncio
async def test_process_frames_successful_with_heatmap():
    """Test process_frames successfully extracts frames using heatmap strategy."""
    from atlas.models import Video

    video = Video(id="VIDEO_002", title="Test Video with Heatmap")
    mock_video_info = {
        "url": "https://example.com/video.mp4",
        "duration": 200.0,
        "chapters": [],
        "heatmap": [
            {"start_time": 10.0, "value": 0.9},
        ],
    }
    fake_webp = FAKE_WEBP

    with (
        patch("maia.painter.flow.VideoRepository") as MockRepo,
        patch("maia.painter.flow.StealthVideoStreamer") as MockStreamer,
        patch("maia.painter.flow.subprocess.run", side_effect=_fake_ffmpeg(fake_webp)),
        patch("maia.painter.flow.get_vault") as mock_get_vault,
        patch("maia.painter.flow.vault_op_with_retry", new_callable=AsyncMock) as mock_vault_retry,
    ):
        mock_vault = mock_get_vault.return_value
        mock_repo = MockRepo.return_value
        mock_repo.mark_visuals_safe = AsyncMock()
        mock_repo.mark_step_failed = AsyncMock()

        mock_streamer_instance = MockStreamer.return_value
        mock_streamer_instance.extract_info = MagicMock(return_value=mock_video_info)
        mock_streamer_instance.extract_heatmap_peaks = MagicMock(return_value=[10.0])

        mock_vault.store_visual_evidence = MagicMock()

        result = await process_frames_task(video)

        assert isinstance(result, tuple)
        assert result[0] == "VIDEO_002"
        assert len(result[1]) > 0
        mock_vault_retry.assert_not_called()
        mock_vault.store_visual_evidence.assert_not_called()
        mock_repo.mark_visuals_safe.assert_not_called()


@pytest.mark.asyncio
async def test_process_frames_fallback_strategy():
    """Test process_frames uses fallback strategy when no chapters/heatmap."""
    from atlas.models import Video

    video = Video(id="VIDEO_003", title="Video without chapters or heatmap")
    mock_video_info = {
        "url": "https://example.com/video.mp4",
        "duration": 600.0,
        "chapters": [],
        "heatmap": [],
    }
    fake_webp = FAKE_WEBP

    with (
        patch("maia.painter.flow.VideoRepository") as MockRepo,
        patch("maia.painter.flow.StealthVideoStreamer") as MockStreamer,
        patch("maia.painter.flow.subprocess.run", side_effect=_fake_ffmpeg(fake_webp)),
        patch("maia.painter.flow.get_vault") as mock_get_vault,
        patch("maia.painter.flow.vault_op_with_retry", new_callable=AsyncMock) as mock_vault_retry,
    ):
        mock_vault = mock_get_vault.return_value
        mock_repo = MockRepo.return_value
        mock_repo.mark_visuals_safe = AsyncMock()
        mock_repo.mark_step_failed = AsyncMock()

        mock_streamer_instance = MockStreamer.return_value
        mock_streamer_instance.extract_info = MagicMock(return_value=mock_video_info)
        mock_streamer_instance.extract_heatmap_peaks = MagicMock(return_value=[])

        mock_vault.store_visual_evidence = MagicMock()

        result = await process_frames_task(video)

        assert isinstance(result, tuple)
        assert result[0] == "VIDEO_003"
        assert len(result[1]) > 0
        mock_vault_retry.assert_not_called()
        mock_vault.store_visual_evidence.assert_not_called()
        mock_repo.mark_visuals_safe.assert_not_called()


@pytest.mark.asyncio
async def test_process_frames_local_meta_no_youtube():
    """Preferred path: read stream URL from vault meta, range-request frames.

    The unified streamer stashes the video's stream URLs in meta/{id}.info.json.
    The painter pulls frames via FFmpeg range requests from that URL — no full
    download and no YouTube metadata session (extract_info must NOT be called).
    """
    from atlas.models import Video

    video = Video(id="VIDEO_LOCAL", title="Local")
    fake_info = {
        "duration": 200.0,
        "chapters": [],
        "heatmap": [],
        "formats": [
            {
                "url": "https://googlevideo.com/segment?id=VIDEO_LOCAL",
                "height": 360,
                "vcodec": "av01.0.05M.08",
                "acodec": "none",
            },
        ],
    }

    with (
        patch("maia.painter.flow.VideoRepository") as MockRepo,
        patch("maia.painter.flow.StealthVideoStreamer") as MockStreamer,
        patch(
            "maia.painter.flow.subprocess.run",
            side_effect=_fake_subprocess(FAKE_WEBP, video_stream=True, duration=200.0),
        ),
        patch("maia.painter.flow.get_vault") as mock_get_vault,
    ):
        mock_repo = MockRepo.return_value
        mock_repo.mark_visuals_safe = AsyncMock()
        mock_repo.mark_step_failed = AsyncMock()
        mock_vault = mock_get_vault.return_value

        # meta present (with stream URLs); raw absent.
        def _fetch(path: str):
            if path == "meta/VIDEO_LOCAL.info.json":
                return io.BytesIO(json.dumps(fake_info).encode())
            return None

        mock_vault.fetch_binary = MagicMock(side_effect=_fetch)

        result = await process_frames_task(video)

        assert isinstance(result, tuple)
        assert result[0] == "VIDEO_LOCAL"
        assert len(result[1]) > 0
        # The YouTube streamer must not be used on the vault-meta path.
        MockStreamer.return_value.extract_info.assert_not_called()
        assert [call.args[0] for call in mock_vault.fetch_binary.call_args_list] == [
            "meta/VI/VIDEO_LOCAL.info.json",
            "meta/VIDEO_LOCAL.info.json",
        ]


@pytest.mark.asyncio
async def test_process_frames_rate_limit_releases_visuals_only():
    """A Painter rate limit must preserve the fetched raw artifact state."""
    from atlas.models import Video

    video = Video(id="VIDEO_RATE_LIMIT", title="Rate limited")

    with (
        patch("maia.painter.flow.VideoRepository") as MockRepo,
        patch("maia.painter.flow.StealthVideoStreamer") as MockStreamer,
    ):
        mock_repo = MockRepo.return_value
        mock_repo.release_visuals_to_pending = AsyncMock()
        mock_repo.release_raw_to_pending = AsyncMock()
        MockStreamer.return_value.extract_info = MagicMock(
            side_effect=StreamRateLimitError("HTTP 429")
        )

        result = await process_frames_task(video)

    assert result is None
    mock_repo.release_visuals_to_pending.assert_awaited_once_with("VIDEO_RATE_LIMIT")
    mock_repo.release_raw_to_pending.assert_not_awaited()


async def test_process_frames_handles_no_stream_url():
    """Test process_frames handles videos with no stream URL."""
    from atlas.models import Video

    video = Video(id="VIDEO_NO_STREAM", title="Video without stream")
    mock_video_info = {"url": None, "chapters": [], "heatmap": []}

    with (
        patch("maia.painter.flow.VideoRepository") as MockRepo,
        patch("maia.painter.flow.StealthVideoStreamer") as MockStreamer,
    ):
        mock_repo = MockRepo.return_value
        mock_repo.mark_step_failed = AsyncMock()
        mock_streamer_instance = MockStreamer.return_value
        mock_streamer_instance.extract_info = MagicMock(return_value=mock_video_info)

        await process_frames_task(video)

        mock_repo.mark_step_failed.assert_called_once_with("VIDEO_NO_STREAM", "visuals")


@pytest.mark.asyncio
async def test_process_frames_handles_video_capture_failure():
    """Test process_frames handles FFmpeg extraction failures."""
    from atlas.models import Video

    video = Video(id="VIDEO_001", title="Test Video")
    mock_video_info = {
        "url": "https://example.com/video.mp4",
        "duration": 100.0,
        "chapters": [{"start_time": 0.0}],
        "heatmap": [],
    }

    fake_webp = FAKE_WEBP

    with (
        patch("maia.painter.flow.VideoRepository") as MockRepo,
        patch("maia.painter.flow.StealthVideoStreamer") as MockStreamer,
        patch(
            "maia.painter.flow.subprocess.run",
            side_effect=_fake_ffmpeg(fake_webp, returncode=1),
        ),
    ):
        mock_repo = MockRepo.return_value
        mock_repo.mark_step_failed = AsyncMock()
        mock_streamer_instance = MockStreamer.return_value
        mock_streamer_instance.extract_info = MagicMock(return_value=mock_video_info)

        await process_frames_task(video)

        mock_repo.mark_step_failed.assert_called_once_with("VIDEO_001", "visuals")


@pytest.mark.asyncio
async def test_run_painter_cycle_handles_vault_failure():
    """Test run_painter_cycle marks videos FAILED when the batched vault write fails."""
    from atlas.models import Video

    mock_videos = [Video(id="VIDEO_001", title="Test Video")]

    with (
        patch("maia.painter.flow.fetch_painter_targets_task", new_callable=AsyncMock) as mock_fetch,
        patch(
            "maia.painter.flow.process_frames_task",
            new_callable=AsyncMock,
            return_value=("VIDEO_001", [(0, FAKE_WEBP)]),
        ),
        patch("maia.painter.flow.VideoRepository") as MockRepo,
        patch("maia.painter.flow.get_vault") as mock_get_vault,
        patch(
            "maia.painter.flow.vault_op_with_retry",
            new_callable=AsyncMock,
            side_effect=Exception("Vault error"),
        ),
        patch("maia.base.clear_quota_exhausted"),
    ):
        mock_fetch.return_value = mock_videos
        mock_repo = MockRepo.return_value
        mock_repo.mark_visuals_safe = AsyncMock()
        mock_repo.mark_step_failed = AsyncMock()
        mock_vault = mock_get_vault.return_value
        mock_vault.store_visual_evidence_batch = MagicMock()

        await painter_flow.fn(batch_size=1)

        # The batched store raised, so every extracted video is marked FAILED.
        mock_repo.mark_step_failed.assert_called_once_with("VIDEO_001", "visuals")
        mock_repo.mark_visuals_safe.assert_not_called()


@pytest.mark.asyncio
async def test_process_frames_propagates_resiliency_strategy():
    """Test process_frames propagates QuotaExhaustedError."""
    from atlas.models import Video

    video = Video(id="VIDEO_001", title="Test Video")

    with (
        patch("maia.painter.flow.VideoRepository"),
        patch("maia.painter.flow.StealthVideoStreamer") as MockStreamer,
        patch("maia.painter.flow.notify_quota_exhausted", new_callable=AsyncMock) as notify,
    ):
        mock_streamer_instance = MockStreamer.return_value
        mock_streamer_instance.extract_info = MagicMock(
            side_effect=QuotaExhaustedError("All keys exhausted")
        )

        with pytest.raises(QuotaExhaustedError):
            await process_frames_task(video)
        notify.assert_awaited_once_with("painter")


@pytest.mark.asyncio
async def test_run_painter_cycle_empty_queue():
    """Test run_painter_cycle handles empty queue gracefully."""
    with patch(
        "maia.painter.flow.fetch_painter_targets_task", new_callable=AsyncMock
    ) as mock_fetch:
        mock_fetch.return_value = []

        await painter_flow.fn(batch_size=5)

        mock_fetch.assert_called_once_with(5)


@pytest.mark.asyncio
async def test_run_painter_cycle_processes_batch():
    """Test run_painter_cycle processes a batch of videos sequentially."""
    from atlas.models import Video

    mock_videos = [
        Video(id="VIDEO_001", title="Video 1"),
        Video(id="VIDEO_002", title="Video 2"),
    ]

    with (
        patch("maia.painter.flow.fetch_painter_targets_task", new_callable=AsyncMock) as mock_fetch,
        patch("maia.painter.flow.process_frames_task", new_callable=AsyncMock) as mock_process,
    ):
        mock_fetch.return_value = mock_videos
        mock_process.return_value = None

        await painter_flow.fn(batch_size=2)

        assert mock_process.call_count == 2
        mock_process.assert_any_call(mock_videos[0])
        mock_process.assert_any_call(mock_videos[1])
