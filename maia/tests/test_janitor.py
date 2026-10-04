"""
Tests for Maia Janitor module — State Machine cleanup cycle.
"""

from typing import Any
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest
from maia.janitor.flow import (
    archive_cold_stats_task,
    handoff_phase_task,
    janitor_flow,
    janitor_operation,
    log_summary_task,
    purge_prefect_runs_task,
    sweep_phase_task,
    vault_flush_task,
)

from tiered_storage import StagedItem


async def test_stats_archival_has_a_cycle_budget():
    with patch("maia.janitor.flow.VideoRepository") as repository:
        repository.return_value.archive_cold_stats = AsyncMock(return_value=5000)
        result = await archive_cold_stats_task(max_batches=2)
    assert result == {"archived": 10000, "batches": 2}
    assert repository.return_value.archive_cold_stats.await_count == 2


async def test_dry_run_skips_storage_and_hygiene_mutations():
    with (
        patch("maia.janitor.flow.refresh_key_pools_task", new_callable=AsyncMock) as refresh,
        patch("maia.janitor.flow.cull_search_queue_task", new_callable=AsyncMock) as cull,
        patch("maia.janitor.flow.vault_flush_task", new_callable=AsyncMock) as flush,
        patch("maia.janitor.flow.sweep_phase_task", new_callable=AsyncMock, return_value=[]),
        patch("maia.janitor.flow.reap_zombie_runs_task", new_callable=AsyncMock) as reap,
        patch("maia.janitor.flow.purge_prefect_runs_task", new_callable=AsyncMock) as purge,
        patch("maia.janitor.flow.events.emit", new_callable=AsyncMock) as emit,
    ):
        await janitor_operation(dry_run=True, prefect_hygiene=True)
    for operation in (refresh, cull, flush, reap, purge, emit):
        operation.assert_not_awaited()


@pytest.fixture(autouse=True)
def mock_optional_prefect_and_preflight_steps():
    """Keep janitor unit tests independent of PostgreSQL and Prefect API."""
    with (
        patch(
            "maia.janitor.flow.refresh_key_pools_task",
            new_callable=AsyncMock,
            return_value={},
        ),
        patch(
            "maia.janitor.flow.cull_search_queue_task",
            new_callable=AsyncMock,
            return_value={"culled": 0},
        ),
        patch(
            "maia.janitor.flow.vault_flush_task",
            new_callable=AsyncMock,
            return_value={"flushed": 0, "failed": 0},
        ),
        patch(
            "maia.janitor.flow.reap_zombie_runs_task",
            new_callable=AsyncMock,
            return_value={"reaped": 0},
        ),
        patch(
            "maia.janitor.flow.purge_prefect_runs_task",
            new_callable=AsyncMock,
            return_value={"purged_flow_runs": 0},
        ),
    ):
        yield


def _make_video(video_id: str, **overrides: Any) -> Any:
    from atlas.models import Video

    return Video(
        id=video_id,
        channel_id="mock_channel",
        title=f"Video {video_id}",
        status="PROCESSED",
        has_transcript=True,
        has_visuals=True,
        **overrides,
    )


def _make_video_dict(video_id: str, **overrides: Any) -> dict[str, Any]:
    return _make_video(video_id, **overrides).model_dump()


@pytest.mark.asyncio
async def test_purge_prefect_runs_closes_http_client():
    """Prefect-history cleanup must close its client on every loop outcome."""
    response_with_rows = MagicMock(status_code=200)
    response_with_rows.json.return_value = {"deleted": ["run-1"]}
    response_empty = MagicMock(status_code=200)
    response_empty.json.return_value = {"deleted": []}

    client = MagicMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    client.post = AsyncMock(side_effect=[response_with_rows, response_empty])

    with patch("httpx.AsyncClient", return_value=client):
        result = await purge_prefect_runs_task(batch_limit=1, max_batches=2)

    assert result == {"purged_flow_runs": 1}
    client.__aenter__.assert_awaited_once()
    client.__aexit__.assert_awaited_once()


@pytest.mark.asyncio
async def test_plain_janitor_skips_prefect_hygiene():
    """The live application operation does not require the Prefect API."""
    with (
        patch("maia.janitor.flow.reap_zombie_runs_task", new_callable=AsyncMock) as reap,
        patch("maia.janitor.flow.purge_prefect_runs_task", new_callable=AsyncMock) as purge,
        patch(
            "maia.janitor.flow.sweep_phase_task",
            new_callable=AsyncMock,
            return_value=[],
        ),
        patch("maia.janitor.flow.log_summary_task", new_callable=AsyncMock),
    ):
        result = await janitor_operation(dry_run=True, archive_stats=False)

    assert result["zombie_runs_reaped"] == 0
    assert result["purged"] == 0
    reap.assert_not_awaited()
    purge.assert_not_awaited()


@pytest.mark.asyncio
async def test_sweep_phase_empty(mock_prefect_logger):
    """Sweep returns empty list when no PROCESSED videos are eligible."""
    with patch("maia.janitor.flow.VideoRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.count_archivable = AsyncMock(return_value=0)
        mock_repo.sweep_archivable = AsyncMock(return_value=[])

        result = await sweep_phase_task(batch_size=50)

        assert result == []
        mock_repo.count_archivable.assert_not_awaited()
        mock_repo.sweep_archivable.assert_awaited_once_with(batch_size=50)


@pytest.mark.asyncio
async def test_sweep_phase_with_videos(mock_prefect_logger):
    """Sweep returns PROCESSED videos eligible for archival."""
    with patch("maia.janitor.flow.VideoRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.count_archivable = AsyncMock(return_value=3)
        mock_repo.sweep_archivable = AsyncMock(
            return_value=[
                _make_video("VIDEO_001"),
                _make_video("VIDEO_002"),
                _make_video("VIDEO_003"),
            ]
        )

        result = await sweep_phase_task(batch_size=50)

        assert len(result) == 3
        assert result[0]["id"] == "VIDEO_001"
        mock_repo.sweep_archivable.assert_called_once_with(batch_size=50)


@pytest.mark.asyncio
async def test_handoff_phase_dry_run(mock_prefect_logger):
    """Hand-off in dry-run mode returns would_archive count."""
    videos = [
        _make_video_dict("VIDEO_001"),
        _make_video_dict("VIDEO_002"),
    ]

    with (
        patch("maia.janitor.flow.VideoRepository") as MockRepo,
        patch("maia.janitor.flow.events") as mock_events,
    ):
        mock_events.emit = AsyncMock()
        mock_repo = MockRepo.return_value
        mock_repo.archive_video_batch = AsyncMock(
            return_value={
                "archived": 0,
                "dry_run": True,
                "would_archive": 2,
                "video_ids": ["VIDEO_001", "VIDEO_002"],
                "failed": 0,
                "failed_ids": [],
            }
        )

        result = await handoff_phase_task(videos, dry_run=True)

        assert result["dry_run"] is True
        assert result["would_archive"] == 2
        mock_repo.archive_video_batch.assert_called_once()


@pytest.mark.asyncio
async def test_janitor_flow_no_videos(mock_prefect_logger):
    """Full janitor cycle with no archivable videos completes gracefully."""
    with (
        patch("maia.janitor.flow.VideoRepository") as MockRepo,
        patch("maia.janitor.flow.TranscriptRepository") as MockTranscriptRepo,
        patch("maia.janitor.flow.get_vault") as mock_vault,
        patch("maia.janitor.flow.archive_cold_stats_task", new_callable=AsyncMock) as mock_stats,
        patch("maia.janitor.flow.events") as mock_events,
    ):
        mock_events.emit = AsyncMock()
        mock_repo = MockRepo.return_value
        mock_transcript_repo = MockTranscriptRepo.return_value
        mock_transcript_repo.pending = AsyncMock(return_value=[])
        mock_transcript_repo.finalize = AsyncMock()
        mock_vault.return_value.store_batch = MagicMock()
        mock_repo.count_archivable = AsyncMock(return_value=0)
        mock_repo.sweep_archivable = AsyncMock(return_value=[])
        mock_stats.return_value = {"archived": 0, "batches": 0}

        result = await janitor_flow.fn(dry_run=False, archive_stats=True, batch_size=50)

        assert result["stats_archived"] == 0
        assert result["videos_archived"] == 0
        assert result["videos_failed"] == 0
        assert result["dry_run"] is False


@pytest.mark.asyncio
async def test_janitor_flow_happy_path(mock_prefect_logger):
    """Full janitor cycle succeeds: stats archived + videos archived."""
    mock_videos = [
        _make_video("VIDEO_001"),
        _make_video("VIDEO_002"),
    ]

    with (
        patch("maia.janitor.flow.VideoRepository") as MockRepo,
        patch("maia.janitor.flow.TranscriptRepository") as MockTranscriptRepo,
        patch("maia.janitor.flow.get_vault") as mock_vault,
        patch("maia.janitor.flow.archive_cold_stats_task", new_callable=AsyncMock) as mock_stats,
        patch("maia.janitor.flow.events") as mock_events,
    ):
        mock_events.emit = AsyncMock()
        mock_repo = MockRepo.return_value
        mock_transcript_repo = MockTranscriptRepo.return_value
        mock_transcript_repo.pending = AsyncMock(return_value=[])
        mock_transcript_repo.finalize = AsyncMock()
        mock_vault.return_value.store_batch = MagicMock()
        mock_repo.count_archivable = AsyncMock(return_value=2)
        mock_repo.sweep_archivable = AsyncMock(return_value=mock_videos)
        mock_repo.archive_video_batch = AsyncMock(
            return_value={"archived": 2, "failed": 0, "failed_ids": []}
        )
        mock_stats.return_value = {"archived": 100, "batches": 1}

        result = await janitor_flow.fn(dry_run=False, archive_stats=True, batch_size=50)

        assert result["stats_archived"] == 100
        assert result["videos_archived"] == 2
        assert result["videos_failed"] == 0
        mock_repo.sweep_archivable.assert_called_once_with(batch_size=50)
        mock_repo.archive_video_batch.assert_called_once()


@pytest.mark.asyncio
async def test_janitor_flow_dry_run(mock_prefect_logger):
    """Dry-run mode does not execute stats archival or video hand-off."""
    with (
        patch("maia.janitor.flow.VideoRepository") as MockRepo,
        patch("maia.janitor.flow.TranscriptRepository") as MockTranscriptRepo,
        patch("maia.janitor.flow.get_vault") as mock_vault,
        patch("maia.janitor.flow.archive_cold_stats_task", new_callable=AsyncMock) as mock_stats,
        patch("maia.janitor.flow.events") as mock_events,
    ):
        mock_events.emit = AsyncMock()
        mock_repo = MockRepo.return_value
        mock_transcript_repo = MockTranscriptRepo.return_value
        mock_transcript_repo.pending = AsyncMock(return_value=[])
        mock_transcript_repo.finalize = AsyncMock()
        mock_vault.return_value.store_batch = MagicMock()
        mock_repo.count_archivable = AsyncMock(return_value=0)
        mock_repo.sweep_archivable = AsyncMock(return_value=[])

        result = await janitor_flow.fn(dry_run=True, archive_stats=True, batch_size=50)

        assert result["dry_run"] is True
        mock_stats.assert_not_called()


@pytest.mark.asyncio
async def test_janitor_flow_failure_path(mock_prefect_logger):
    """When archive_video_batch reports failures, cycle still reports results."""
    mock_videos = [
        _make_video("VIDEO_FAIL_001"),
        _make_video("VIDEO_FAIL_002"),
    ]

    with (
        patch("maia.janitor.flow.VideoRepository") as MockRepo,
        patch("maia.janitor.flow.TranscriptRepository") as MockTranscriptRepo,
        patch("maia.janitor.flow.get_vault") as mock_vault,
        patch("maia.janitor.flow.archive_cold_stats_task", new_callable=AsyncMock) as mock_stats,
        patch("maia.janitor.flow.events") as mock_events,
    ):
        mock_events.emit = AsyncMock()
        mock_repo = MockRepo.return_value
        mock_transcript_repo = MockTranscriptRepo.return_value
        mock_transcript_repo.pending = AsyncMock(return_value=[])
        mock_transcript_repo.finalize = AsyncMock()
        mock_vault.return_value.store_batch = MagicMock()
        mock_repo.count_archivable = AsyncMock(return_value=2)
        mock_repo.sweep_archivable = AsyncMock(return_value=mock_videos)
        mock_repo.archive_video_batch = AsyncMock(
            return_value={
                "archived": 0,
                "failed": 2,
                "failed_ids": ["VIDEO_FAIL_001", "VIDEO_FAIL_002"],
            }
        )
        mock_stats.return_value = {"archived": 50, "batches": 1}

        result = await janitor_flow.fn(dry_run=False, archive_stats=True, batch_size=50)

        assert result["videos_archived"] == 0
        assert result["videos_failed"] == 2


@pytest.mark.asyncio
async def test_log_summary_emits_event(mock_prefect_logger):
    """log_summary_task emits a janitor.cycle_complete event."""
    with patch("maia.janitor.flow.events") as mock_events:
        mock_events.emit = AsyncMock()
        results = {
            "stats_archived": 50,
            "videos_archived": 10,
            "videos_failed": 0,
            "dry_run": False,
        }

        await log_summary_task(results)

        mock_events.emit.assert_called_once_with(
            "janitor.cycle_complete",
            "janitor",
            {
                "stats_archived": 50,
                "videos_archived": 10,
                "videos_failed": 0,
                "dry_run": False,
            },
        )


@pytest.mark.asyncio
async def test_archive_video_batch_delegates_to_repo(mock_prefect_logger):
    """handoff_phase_task correctly delegates archive_video_batch to repo."""
    videos = [_make_video_dict("VIDEO_001")]

    with (
        patch("maia.janitor.flow.VideoRepository") as MockRepo,
        patch("maia.janitor.flow.events") as mock_events,
    ):
        mock_events.emit = AsyncMock()
        mock_repo = MockRepo.return_value
        mock_repo.archive_video_batch = AsyncMock(
            return_value={"archived": 1, "failed": 0, "failed_ids": []}
        )

        result = await handoff_phase_task(videos, dry_run=False)

        assert result["archived"] == 1
        mock_repo.archive_video_batch.assert_called_once_with(ANY, dry_run=False)


@pytest.mark.asyncio
async def test_vault_flush_keeps_hot_payload_when_store_returns_no_matching_uri():
    """A malformed vault result must leave the row pending and recoverable."""
    pending = [
        StagedItem("VIDEO_001", "1", b"{}", namespace="transcripts/VI/VIDEO_001", suffix=".json")
    ]

    with (
        patch("maia.janitor.flow.TranscriptRepository") as MockTranscriptRepo,
        patch("maia.janitor.flow.get_vault") as mock_get_vault,
    ):
        transcript_repo = MockTranscriptRepo.return_value
        transcript_repo.pending = AsyncMock(return_value=pending)
        transcript_repo.finalize = AsyncMock()
        mock_get_vault.return_value.store_batch = MagicMock(
            return_value=["hf://datasets/test/transcripts/XX/OTHER.json"]
        )

        result = await vault_flush_task(batch_size=1)

    assert result == {"flushed": 0, "failed": 1}
    transcript_repo.finalize.assert_not_awaited()


@pytest.mark.asyncio
async def test_transcript_reconciliation_has_independent_count_budget():
    from types import SimpleNamespace

    from maia.janitor.flow import flush_transcripts_task

    policy = SimpleNamespace(
        JANITOR_TRANSCRIPT_BATCH_SIZE=100,
        JANITOR_TRANSCRIPT_MAX_BATCHES=3,
        JANITOR_TRANSCRIPT_BUDGET_SECONDS=120,
    )
    with (
        patch("maia.janitor.flow.get_settings", return_value=policy),
        patch(
            "maia.janitor.flow.vault_flush_task",
            AsyncMock(return_value={"flushed": 100, "failed": 0}),
        ) as flush,
    ):
        result = await flush_transcripts_task()
    assert result == {"flushed": 300, "failed": 0, "batches": 3}
    assert flush.await_count == 3
    assert all(call.args == (100,) for call in flush.await_args_list)


@pytest.mark.asyncio
async def test_transcript_reconciliation_stops_on_failure_or_elapsed_budget():
    from types import SimpleNamespace

    from maia.janitor.flow import flush_transcripts_task

    policy = SimpleNamespace(
        JANITOR_TRANSCRIPT_BATCH_SIZE=100,
        JANITOR_TRANSCRIPT_MAX_BATCHES=10,
        JANITOR_TRANSCRIPT_BUDGET_SECONDS=120,
    )
    for response, times in [
        ({"flushed": 90, "failed": 10}, [0, 0]),
        ({"flushed": 100, "failed": 0}, [0, 0, 121]),
    ]:
        with (
            patch("maia.janitor.flow.get_settings", return_value=policy),
            patch("maia.janitor.flow.time.monotonic", side_effect=times),
            patch("maia.janitor.flow.vault_flush_task", AsyncMock(return_value=response)) as flush,
        ):
            result = await flush_transcripts_task()
        assert result["batches"] == 1
        flush.assert_awaited_once()
