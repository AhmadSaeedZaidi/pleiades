"""Tests for the VideoStateMixin streamer/singer claim + mark methods."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from atlas.models import Video
from atlas.repositories.video.state_machine import VideoStateMixin


class FakeState(VideoStateMixin):
    """VideoStateMixin with the low-level DB helpers stubbed out."""

    def __init__(self) -> None:
        self._fetch_all = AsyncMock()
        self._fetch_one = AsyncMock()
        self._execute = AsyncMock()


@pytest.mark.asyncio
async def test_claim_streamer_batch():
    s = FakeState()
    s._fetch_all.return_value = [{"id": "V1", "title": "t", "has_audio": False}]
    vids = await s.claim_streamer_batch(5)
    s._fetch_all.assert_called_once()
    assert len(vids) == 1
    assert vids[0].id == "V1"
    assert isinstance(vids[0], Video)


@pytest.mark.asyncio
async def test_claim_singer_batch():
    s = FakeState()
    s._fetch_all.return_value = [
        {"id": "V1", "title": "t", "has_audio": True, "has_transcript": False}
    ]
    vids = await s.claim_singer_batch(5)
    s._fetch_all.assert_called_once()
    assert vids[0].id == "V1"
    # Singer also reclaims PROCESSED rows that are missing audio (self-heal),
    # not just PENDING/PROCESSING.
    sql = s._fetch_all.call_args[0][0]
    assert "'PROCESSED'" in sql
    assert "has_audio = FALSE" in sql


@pytest.mark.asyncio
async def test_mark_audio_safe():
    s = FakeState()
    await s.mark_audio_safe("V1")
    s._execute.assert_called_once()
    sql = s._execute.call_args[0][0]
    assert "has_audio = TRUE" in sql
    # Idempotent: a DONE step is never re-marked.
    assert "audio_phase <> 'DONE'" in sql
    # If audio was the last missing artifact, latch PROCESSED now.
    assert "has_visuals AND has_transcript THEN 'PROCESSED'" in sql
    assert s._execute.call_args[0][1][1] == "V1"


@pytest.mark.asyncio
async def test_mark_transcript_safe_requires_audio_for_processed():
    s = FakeState()
    await s.mark_transcript_safe("V1")
    sql = s._execute.call_args[0][0]
    # caption-first scribe runs in parallel with the singer, so PROCESSED may
    # only latch once audio is also present.
    assert "has_visuals AND has_audio THEN 'PROCESSED'" in sql


@pytest.mark.asyncio
async def test_mark_visuals_safe_requires_audio_for_processed():
    s = FakeState()
    await s.mark_visuals_safe("V1")
    sql = s._execute.call_args[0][0]
    assert "has_transcript AND has_audio THEN 'PROCESSED'" in sql


@pytest.mark.asyncio
async def test_claim_muralist_batch():
    s = FakeState()
    s._fetch_all.return_value = [{"id": "V1", "title": "t", "has_video": False}]
    vids = await s.claim_muralist_batch(5)
    s._fetch_all.assert_called_once()
    assert vids[0].id == "V1"


@pytest.mark.asyncio
async def test_mark_video_safe():
    s = FakeState()
    await s.mark_video_safe("V1")
    s._execute.assert_called_once()
    sql = s._execute.call_args[0][0]
    assert "has_video = TRUE" in sql
    # Idempotent: clip already DONE → no-op.
    assert "clip_phase <> 'DONE'" in sql
    assert s._execute.call_args[0][1][1] == "V1"


@pytest.mark.asyncio
async def test_mark_fetched():
    s = FakeState()
    await s.mark_fetched("V1", "raw/V1.mp4")
    s._execute.assert_called_once()
    sql, params = s._execute.call_args[0]
    assert "fetched = TRUE" in sql
    assert "raw_uri = %s" in sql
    assert "raw_stored_at = %s" in sql
    # Idempotent: already-fetched raw is never re-marked.
    assert "raw_phase <> 'DONE'" in sql
    assert "captions_uri" not in sql
    assert "has_captions" not in sql
    assert params[0] == "raw/V1.mp4"
    assert params[3] == "V1"


@pytest.mark.asyncio
async def test_reclaim_raw_if_complete_deletes_when_both_done():
    s = FakeState()
    s._fetch_one.return_value = {
        "raw_uri": "raw/V1/V1.mp4",
        "audio_phase": "DONE",
        "visuals_phase": "DONE",
        "clip_phase": "DONE",
        "raw_stored_at": datetime.now(UTC),
    }
    with patch("atlas.vault.get_vault") as mock_gv:
        mock_vault = mock_gv.return_value
        mock_vault.delete_files = MagicMock(return_value=1)

        deleted = await s.reclaim_raw_if_complete("V1")

    assert deleted == 1
    mock_vault.delete_files.assert_called_once_with(["raw/V1/V1.mp4", "meta/V1/V1.info.json"])
    # Pointer cleared so we never reclaim twice.
    s._execute.assert_called_once()
    assert s._execute.call_args[0][0].startswith("UPDATE videos SET raw_uri = NULL")


@pytest.mark.asyncio
async def test_reclaim_raw_if_complete_skips_when_incomplete():
    s = FakeState()
    s._fetch_one.return_value = {
        "raw_uri": "raw/V1.mp4",
        "audio_phase": "DONE",
        "visuals_phase": "PENDING",  # painter hasn't derived frames yet → join not met
        "clip_phase": "PENDING",
    }
    deleted = await s.reclaim_raw_if_complete("V1")
    assert deleted == 0
    s._execute.assert_not_called()


@pytest.mark.asyncio
async def test_reclaim_raw_keeps_raw_within_ttl_when_no_clip():
    """Muralist hasn't run (clip not DONE) and raw is fresh → keep raw."""
    s = FakeState()
    s._fetch_one.return_value = {
        "raw_uri": "raw/V1.mp4",
        "audio_phase": "DONE",
        "visuals_phase": "DONE",
        "clip_phase": "PENDING",
        "raw_stored_at": datetime.now(UTC) - timedelta(hours=10),  # < 48h TTL
    }
    deleted = await s.reclaim_raw_if_complete("V1")
    assert deleted == 0
    s._execute.assert_not_called()


@pytest.mark.asyncio
async def test_reclaim_raw_reclaims_after_ttl_when_no_clip():
    """Muralist never ran but raw aged past TTL → reclaim to bound disk."""
    s = FakeState()
    s._fetch_one.return_value = {
        "raw_uri": "raw/V1.mp4",
        "audio_phase": "DONE",
        "visuals_phase": "DONE",
        "clip_phase": "PENDING",
        "raw_stored_at": datetime.now(UTC) - timedelta(hours=100),  # > 48h TTL
    }
    with (
        patch("atlas.vault.get_vault") as mock_gv,
        patch("atlas.vault.meta_path", return_value="meta/V1.info.json"),
    ):
        mock_vault = mock_gv.return_value
        mock_vault.delete_files = MagicMock(return_value=1)
        deleted = await s.reclaim_raw_if_complete("V1")
    assert deleted == 1
    mock_vault.delete_files.assert_called_once_with(["raw/V1.mp4", "meta/V1.info.json"])
    s._execute.assert_called_once()
    assert s._execute.call_args[0][0].startswith("UPDATE videos SET raw_uri = NULL")


@pytest.mark.asyncio
async def test_reclaim_raw_keeps_when_stored_at_null_and_no_clip():
    """Rows predating raw_stored_at are never reclaimed via age."""
    s = FakeState()
    s._fetch_one.return_value = {
        "raw_uri": "raw/V1.mp4",
        "audio_phase": "DONE",
        "visuals_phase": "DONE",
        "clip_phase": "PENDING",
        "raw_stored_at": None,
    }
    deleted = await s.reclaim_raw_if_complete("V1")
    assert deleted == 0
    s._execute.assert_not_called()


@pytest.mark.asyncio
async def test_reclaim_raw_join_barrier_requires_both_mandatory_steps():
    """The join barrier is over phase columns, not booleans. Missing painter
    (visuals not DONE) must block reclamation even if audio is done."""
    s = FakeState()
    s._fetch_one.return_value = {
        "raw_uri": "raw/V1.mp4",
        "audio_phase": "DONE",
        "visuals_phase": "PROCESSING",  # in-flight, not joined
        "clip_phase": "DONE",
        "raw_stored_at": datetime.now(UTC) - timedelta(hours=200),  # way past TTL
    }
    deleted = await s.reclaim_raw_if_complete("V1")
    assert deleted == 0
    s._execute.assert_not_called()


@pytest.mark.asyncio
async def test_claim_singer_sets_audio_processing_phase():
    s = FakeState()
    s._fetch_all.return_value = []
    await s.claim_singer_batch(5)
    sql = s._fetch_all.call_args[0][0]
    assert "audio_phase = 'PROCESSING'" in sql
    assert "fetched = TRUE" in sql
    assert "has_audio = FALSE" in sql


@pytest.mark.asyncio
async def test_claim_scribe_batch_does_not_require_audio():
    """Scribe gets captions from YouTube directly; audio STT is only a paid
    fallback. It must NOT be gated on the singer's audio being present."""
    s = FakeState()
    s._fetch_all.return_value = []
    await s.claim_scribe_batch(5)
    sql = s._fetch_all.call_args[0][0]
    assert "has_audio = TRUE" not in sql
    assert "has_transcript = FALSE" in sql


@pytest.mark.asyncio
async def test_claim_muralist_batch_requires_raw_uri():
    """Muralist needs the raw input — never claim a row whose raw was reclaimed."""
    s = FakeState()
    s._fetch_all.return_value = []
    await s.claim_muralist_batch(5)
    sql = s._fetch_all.call_args[0][0]
    assert "raw_uri IS NOT NULL" in sql
    assert "has_video = FALSE" in sql


@pytest.mark.asyncio
async def test_release_raw_to_pending_resets_only_raw_step():
    s = FakeState()

    await s.release_raw_to_pending("V1")

    s._execute.assert_awaited_once()
    sql, params = s._execute.await_args.args
    assert "raw_phase = 'PENDING'" in sql
    assert "audio_phase" not in sql
    assert params[1] == "V1"


@pytest.mark.asyncio
async def test_release_audio_to_pending_preserves_raw_state():
    s = FakeState()

    await s.release_audio_to_pending("V1")

    s._execute.assert_awaited_once()
    sql, params = s._execute.await_args.args
    assert "audio_phase = 'PENDING'" in sql
    assert "raw_phase" not in sql
    assert "raw_uri" not in sql


@pytest.mark.asyncio
async def test_release_visuals_to_pending_preserves_raw_state():
    s = FakeState()

    await s.release_visuals_to_pending("V1")

    s._execute.assert_awaited_once()
    sql, params = s._execute.await_args.args
    assert "visuals_phase = 'PENDING'" in sql
    assert "raw_phase" not in sql
    assert "raw_uri" not in sql
    assert params[1] == "V1"


@pytest.mark.asyncio
async def test_release_clip_to_pending_preserves_raw_state():
    s = FakeState()

    await s.release_clip_to_pending("V1")

    s._execute.assert_awaited_once()
    sql, params = s._execute.await_args.args
    assert "clip_phase = 'PENDING'" in sql
    assert "raw_phase" not in sql
    assert "raw_uri" not in sql
    assert params[1] == "V1"


@pytest.mark.parametrize(
    ("method_name", "phase"),
    [
        ("release_raw_to_pending", "raw"),
        ("release_audio_to_pending", "audio"),
        ("release_visuals_to_pending", "visuals"),
        ("release_transcript_to_pending", "transcript"),
        ("release_clip_to_pending", "clip"),
    ],
)
@pytest.mark.asyncio
async def test_release_is_conditional_on_current_processing_claim(method_name, phase):
    s = FakeState()
    await getattr(s, method_name)("V1")
    sql = s._execute.await_args.args[0]
    assert f"AND {phase}_phase = 'PROCESSING'" in sql


@pytest.mark.parametrize("step", ["raw", "audio", "visuals", "transcript", "clip"])
@pytest.mark.asyncio
async def test_stage_retry_does_not_demote_completed_lifecycle(step):
    """A transient retry must not hide a processed video from archival."""
    s = FakeState()
    await getattr(s, f"release_{step}_to_pending")("V1")
    sql = s._execute.await_args.args[0]
    assert "status =" not in sql
    assert f"{step}_phase = 'PENDING'" in sql
    assert "status <> 'ARCHIVED'" in sql


@pytest.mark.asyncio
async def test_mark_step_failed_updates_only_current_stage_claim():
    s = FakeState()
    await s.mark_step_failed("V1", "visuals")
    sql, params = s._execute.await_args.args
    assert "visuals_phase = 'FAILED'" in sql
    assert "visuals_phase = 'PROCESSING'" in sql
    assert params[1] == "V1"


@pytest.mark.asyncio
async def test_mark_step_failed_does_not_fail_the_whole_row():
    """A failed stage must not remove the video from the other four consumers.

    Regression: mark_step_failed used to set the row-level ``status='FAILED'``.
    Every claim query filters on ``status IN ('PENDING','PROCESSING'[,...])``, so
    one transient error in one agent permanently stranded the video across the
    whole pipeline with no in-maia recovery path.
    """
    s = FakeState()
    await s.mark_step_failed("V1", "visuals")
    sql, _ = s._execute.await_args.args
    assert "status" not in sql.lower()
    assert "'FAILED'" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "step"),
    [
        ("claim_scribe_batch", "transcript"),
        ("claim_painter_batch", "visuals"),
        ("claim_streamer_batch", "raw"),
        ("claim_singer_batch", "audio"),
        ("claim_muralist_batch", "clip"),
    ],
)
async def test_every_claim_skips_only_its_own_failed_phase(method_name: str, step: str):
    """Each claim excludes its own terminal failure, and no other step's.

    This is the other half of the per-step isolation fix: without it, dropping
    the row-level FAILED would make a permanently-dead stage retry forever.
    """
    s = FakeState()
    s._fetch_all.return_value = []
    await getattr(s, method_name)(5)
    sql, _ = s._fetch_all.await_args.args
    assert f"{step}_phase <> 'FAILED'" in sql
    for other in ("raw", "audio", "visuals", "transcript", "clip"):
        if other != step:
            assert f"{other}_phase <> 'FAILED'" not in sql
