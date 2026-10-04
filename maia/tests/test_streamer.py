"""
Tests for Maia Streamer module (YouTube source fetcher; network pull only).
"""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from atlas.utils import QuotaExhaustedError
from maia.media.streamer import AudioExtractionError
from maia.streamer.flow import (
    fetch_source_task,
    fetch_streamer_targets_task,
    streamer_flow,
)

FAKE_BYTES = b"RAWBYTES" + b"\x00" * 200


@pytest.mark.asyncio
async def test_fetch_streamer_targets_empty():
    with patch("maia.streamer.flow.VideoRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.claim_streamer_batch = AsyncMock(return_value=[])

        result = await fetch_streamer_targets_task(batch_size=5)

        assert result == []
        mock_repo.claim_streamer_batch.assert_called_once_with(5)


@pytest.mark.asyncio
async def test_fetch_streamer_targets_with_videos():
    from atlas.models import Video

    mock_videos = [
        Video(id="VIDEO_001", title="Test Video 1"),
        Video(id="VIDEO_002", title="Test Video 2"),
    ]

    with patch("maia.streamer.flow.VideoRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.claim_streamer_batch = AsyncMock(return_value=mock_videos)

        result = await fetch_streamer_targets_task(batch_size=5)

        assert len(result) == 2
        assert result[0].id == "VIDEO_001"
        mock_repo.claim_streamer_batch.assert_called_once_with(5)


@pytest.mark.asyncio
async def test_fetch_source_successful():
    """fetch_source pulls audio+meta and returns a 4-tuple; no DB write."""
    from atlas.models import Video

    video = Video(id="VIDEO_001", title="Test Video")

    fake_path = MagicMock()
    fake_path.read_bytes = MagicMock(return_value=FAKE_BYTES)
    fake_path.name = "VIDEO_001.webm"

    with (
        patch("maia.streamer.flow.VideoRepository") as MockRepo,
        patch("maia.streamer.flow.StealthVideoStreamer") as MockStreamer,
    ):
        mock_repo = MockRepo.return_value
        mock_repo.mark_fetched = AsyncMock()
        mock_repo.mark_step_failed = AsyncMock()
        mock_repo.release_raw_to_pending = AsyncMock()
        mock_streamer_instance = MockStreamer.return_value
        # download_unified returns (audio_path, info_path).
        mock_streamer_instance.download_unified = MagicMock(return_value=(fake_path, None))

        result = await fetch_source_task(video)

        assert isinstance(result, tuple)
        assert len(result) == 4
        assert result[0] == "VIDEO_001"
        assert result[1] == "raw/VI/VIDEO_001.webm"
        assert result[2] == FAKE_BYTES
        # No metadata produced → that slot is None.
        assert result[3] is None
        # Marking fetched happens in the flow (batched), not here.
        mock_repo.mark_fetched.assert_not_called()
        mock_repo.mark_step_failed.assert_not_called()
        mock_repo.release_raw_to_pending.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_source_successful_with_meta():
    """Metadata produced → meta_bytes slot is the info.json bytes."""
    from atlas.models import Video

    video = Video(id="VIDEO_002", title="Test Video")

    fake_path = MagicMock()
    fake_path.read_bytes = MagicMock(return_value=FAKE_BYTES)
    fake_path.name = "VIDEO_002.webm"
    fake_info = MagicMock()
    fake_info.read_bytes = MagicMock(return_value=b"INFOJSON")

    with (
        patch("maia.streamer.flow.VideoRepository") as MockRepo,
        patch("maia.streamer.flow.StealthVideoStreamer") as MockStreamer,
    ):
        mock_repo = MockRepo.return_value
        mock_repo.mark_fetched = AsyncMock()
        mock_streamer_instance = MockStreamer.return_value
        mock_streamer_instance.download_unified = MagicMock(return_value=(fake_path, fake_info))

        result = await fetch_source_task(video)

        assert result[3] == b"INFOJSON"


@pytest.mark.asyncio
async def test_fetch_source_handles_extraction_failure():
    """AudioExtractionError → released to PENDING (transient), returns None."""
    from atlas.models import Video

    video = Video(id="VIDEO_001", title="Test Video")

    with (
        patch("maia.streamer.flow.VideoRepository") as MockRepo,
        patch("maia.streamer.flow.StealthVideoStreamer") as MockStreamer,
    ):
        mock_repo = MockRepo.return_value
        mock_repo.release_raw_to_pending = AsyncMock()
        mock_repo.mark_step_failed = AsyncMock()
        mock_streamer_instance = MockStreamer.return_value
        mock_streamer_instance.download_unified = MagicMock(
            side_effect=AudioExtractionError("yt-dlp failed")
        )

        result = await fetch_source_task(video)

        assert result is None
        mock_repo.release_raw_to_pending.assert_called_once_with("VIDEO_001")
        mock_repo.mark_step_failed.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_source_releases_quota_to_pending():
    """Quota exhaustion releases the video to PENDING instead of crashing the
    whole service (which would crash-loop on the auto-restart during bulk
    healing)."""
    from atlas.models import Video

    video = Video(id="VIDEO_001", title="Test Video")

    with (
        patch("maia.streamer.flow.VideoRepository") as MockRepo,
        patch("maia.streamer.flow.StealthVideoStreamer") as MockStreamer,
        patch("maia.streamer.flow.notify_quota_exhausted", new_callable=AsyncMock),
    ):
        mock_repo = MockRepo.return_value
        mock_repo.release_raw_to_pending = AsyncMock()
        mock_streamer_instance = MockStreamer.return_value
        mock_streamer_instance.download_unified = MagicMock(
            side_effect=QuotaExhaustedError("All keys exhausted")
        )

        result = await fetch_source_task(video)

        assert result is None
        mock_repo.release_raw_to_pending.assert_awaited_once_with("VIDEO_001")


@pytest.mark.asyncio
async def test_run_streamer_cycle_empty_queue():
    with (
        patch(
            "maia.streamer.flow.fetch_streamer_targets_task", new_callable=AsyncMock
        ) as mock_fetch,
        patch("maia.streamer.flow.get_rate_limit_cooldown_until", return_value=None),
    ):
        mock_fetch.return_value = []

        await streamer_flow.fn(batch_size=5)

        mock_fetch.assert_called_once_with(5)


@pytest.mark.asyncio
async def test_run_streamer_cycle_batched_store():
    """Batched raw sources stored in ONE vault commit; videos marked fetched."""
    from atlas.models import Video

    mock_videos = [Video(id="VIDEO_001", title="T1"), Video(id="VIDEO_002", title="T2")]

    with (
        patch(
            "maia.streamer.flow.fetch_streamer_targets_task", new_callable=AsyncMock
        ) as mock_fetch,
        patch(
            "maia.streamer.flow.fetch_source_task",
            new_callable=AsyncMock,
            side_effect=[
                ("VIDEO_001", "raw/VIDEO_001.webm", FAKE_BYTES, None),
                ("VIDEO_002", "raw/VIDEO_002.webm", FAKE_BYTES, None),
            ],
        ),
        patch("maia.streamer.flow.VideoRepository") as MockRepo,
        patch("maia.streamer.flow.get_vault") as mock_get_vault,
        patch("maia.streamer.flow.vault_op_with_retry", new_callable=AsyncMock) as mock_vault_retry,
        patch("maia.streamer.flow.get_rate_limit_cooldown_until", return_value=None),
        patch("maia.base.clear_quota_exhausted"),
    ):
        mock_fetch.return_value = mock_videos
        mock_repo = MockRepo.return_value
        mock_repo.mark_fetched = AsyncMock()
        mock_repo.mark_step_failed = AsyncMock()
        mock_vault = mock_get_vault.return_value
        mock_vault.store_batch = MagicMock()

        await streamer_flow.fn(batch_size=2)

        # One batched commit for the whole batch.
        mock_vault_retry.assert_called_once()
        assert mock_repo.mark_fetched.call_count == 2
        mock_repo.mark_step_failed.assert_not_called()


@pytest.mark.asyncio
async def test_run_streamer_cycle_vault_failure_marks_failed():
    from atlas.models import Video

    mock_videos = [Video(id="VIDEO_001", title="T1")]

    with (
        patch(
            "maia.streamer.flow.fetch_streamer_targets_task", new_callable=AsyncMock
        ) as mock_fetch,
        patch(
            "maia.streamer.flow.fetch_source_task",
            new_callable=AsyncMock,
            return_value=("VIDEO_001", "raw/VIDEO_001.webm", FAKE_BYTES, None),
        ),
        patch("maia.streamer.flow.VideoRepository") as MockRepo,
        patch("maia.streamer.flow.get_vault") as mock_get_vault,
        patch(
            "maia.streamer.flow.vault_op_with_retry",
            new_callable=AsyncMock,
            side_effect=Exception("Vault error"),
        ),
        patch("maia.streamer.flow.get_rate_limit_cooldown_until", return_value=None),
        patch("maia.base.clear_quota_exhausted"),
    ):
        mock_fetch.return_value = mock_videos
        mock_repo = MockRepo.return_value
        mock_repo.mark_fetched = AsyncMock()
        mock_repo.mark_step_failed = AsyncMock()
        mock_vault = mock_get_vault.return_value
        mock_vault.store_batch = MagicMock()

        await streamer_flow.fn(batch_size=1)

        mock_repo.mark_step_failed.assert_called_once_with("VIDEO_001", "raw")
        mock_repo.mark_fetched.assert_not_called()


def test_download_unified_salvages_audio_when_metadata_429(tmp_path):
    """Audio downloaded, but metadata 429 → still return the audio file + None info."""
    from maia.media.streamer import StealthVideoStreamer

    streamer = StealthVideoStreamer()
    # Simulate yt-dlp writing the audio file before failing on metadata 429.
    (tmp_path / "VID.webm").write_bytes(b"audiodata")

    class _FailedRun:
        returncode = 1
        stdout = ""
        stderr = (
            "ERROR: Unable to download video metadata for 'en': HTTP Error 429: Too Many Requests"
        )

    with patch("maia.media.streamer.subprocess.run", return_value=_FailedRun()):
        audio_path, info = streamer.download_unified("VID", str(tmp_path))

    assert audio_path.name == "VID.webm"
    # The metadata is best-effort; the audio must not be discarded.
    assert info is None


def test_download_unified_command_uses_web_clients_and_audio_fallback(tmp_path):
    """Unified fetch must use the bgutil-compatible clients and format fallback."""
    from maia.media.streamer import StealthVideoStreamer

    (tmp_path / "VID.webm").write_bytes(b"audiodata")

    class _SuccessfulRun:
        returncode = 0
        stdout = ""
        stderr = ""

    streamer = StealthVideoStreamer()
    with patch("maia.media.streamer.subprocess.run", return_value=_SuccessfulRun()) as run:
        audio_path, info = streamer.download_unified("VID", str(tmp_path))

    command = run.call_args.args[0]
    assert audio_path.name == "VID.webm"
    assert info is None
    assert "youtube:player_client=web,tv" in command
    assert command[command.index("-f") + 1] == "bestaudio/best"


@pytest.mark.parametrize(
    ("method", "filename"),
    [("extract_audio", "VID.opus"), ("download_raw", "VID.webm")],
)
def test_audio_commands_default_to_web_clients(tmp_path, method, filename):
    """All audio download entrypoints use the PoToken-compatible client set."""
    from maia.media.streamer import StealthVideoStreamer

    (tmp_path / filename).write_bytes(b"audiodata")

    class _SuccessfulRun:
        returncode = 0
        stdout = ""
        stderr = ""

    streamer = StealthVideoStreamer()
    with patch("maia.media.streamer.subprocess.run", return_value=_SuccessfulRun()) as run:
        getattr(streamer, method)("VID", str(tmp_path))

    command = run.call_args.args[0]
    assert "youtube:player_client=web,tv" in command
    assert command[command.index("-f") + 1] == "bestaudio/best"


def test_download_unified_raises_on_media_failure(tmp_path):
    """No media produced + non-zero exit → hard failure (not salvaged)."""
    from maia.media.streamer import StealthVideoStreamer

    streamer = StealthVideoStreamer()

    class _FailedRun:
        returncode = 1
        stdout = ""
        stderr = "ERROR: [youtube] VID: Video unavailable"

    with (
        patch("maia.media.streamer.subprocess.run", return_value=_FailedRun()),
        pytest.raises(AudioExtractionError),
    ):
        streamer.download_unified("VID", str(tmp_path))


@pytest.mark.asyncio
async def test_streamer_run_skips_during_cooldown():
    """When a rate-limit cooldown is active, run() sits out without claiming."""
    import maia.streamer.flow as sf
    from maia.streamer.flow import StreamerAgent

    future = time.time() + 600
    with (
        patch.object(sf, "get_rate_limit_cooldown_until", return_value=future),
        patch.object(StreamerAgent, "claim_batch", AsyncMock()) as mock_claim,
    ):
        result = await StreamerAgent().run(batch_size=5)

    assert result.get("skipped") is True
    mock_claim.assert_not_awaited()


@pytest.mark.asyncio
async def test_streamer_run_backs_off_after_rate_limit():
    """A rate-limited cycle bumps the cooldown; a clean cycle clears it."""
    import maia.streamer.flow as sf
    from atlas.models import Video
    from maia.streamer.flow import StreamerAgent

    video = Video(id="VID_RL", title="t")
    with (
        patch.object(sf, "get_rate_limit_cooldown_until", return_value=None),
        patch.object(StreamerAgent, "claim_batch", AsyncMock(return_value=[video])),
        patch.object(
            StreamerAgent,
            "process_one",
            AsyncMock(side_effect=lambda v: setattr(sf, "_rate_limit_cycle_hit", True) or None),
        ),
        patch.object(sf, "bump_rate_limit_cooldown", MagicMock()) as mock_bump,
        patch.object(sf, "clear_rate_limit_cooldown", MagicMock()) as mock_clear,
        patch("maia.base.clear_quota_exhausted", MagicMock()),
    ):
        await StreamerAgent().run(batch_size=5)

    mock_bump.assert_called_once_with("streamer")
    mock_clear.assert_not_called()


@pytest.mark.asyncio
async def test_streamer_run_clears_cooldown_on_clean_cycle():
    """A cycle with no rate limits clears any prior cooldown."""
    import maia.streamer.flow as sf
    from atlas.models import Video
    from maia.streamer.flow import StreamerAgent

    video = Video(id="VID_OK", title="t")
    with (
        patch.object(sf, "get_rate_limit_cooldown_until", return_value=None),
        patch.object(StreamerAgent, "claim_batch", AsyncMock(return_value=[video])),
        patch.object(StreamerAgent, "process_one", AsyncMock(return_value=None)),
        patch.object(sf, "bump_rate_limit_cooldown", MagicMock()) as mock_bump,
        patch.object(sf, "clear_rate_limit_cooldown", MagicMock()) as mock_clear,
        patch("maia.base.clear_quota_exhausted", MagicMock()),
    ):
        await StreamerAgent().run(batch_size=5)

    mock_clear.assert_called_once_with("streamer")
    mock_bump.assert_not_called()
