"""A completed loop is not proof of a stored extraction result."""

from unittest.mock import AsyncMock, patch

from atlas.models import Video
from maia.base import BaseBatchAgent
from maia.streamer.flow import StreamerAgent
from maia.telemetry import CycleMonitor


async def test_handled_download_failures_are_not_reported_as_success():
    agent = StreamerAgent()
    agent.throttle_seconds = 0
    agent.claim_batch = AsyncMock(return_value=[Video(id=str(i), title="input") for i in range(3)])
    agent.process_one = AsyncMock(return_value=None)
    with patch("maia.base.clear_quota_exhausted"):
        result = await BaseBatchAgent.run(agent)
    assert result["videos_attempted"] == 3 and result["raw_stored"] == 0
    assert result["videos_succeeded"] == 0 and result["videos_failed"] == 3
    monitor = CycleMonitor()
    monitor.register("streamer", 120)
    monitor.start("streamer")
    monitor.finish("streamer", result)
    assert monitor.snapshot()["cycles"][0]["error"] == "PartialFailure"


async def test_scribe_separates_real_transcripts_notices_and_retryable_failures():
    from maia.scribe.flow import scribe_operation

    with (
        patch(
            "maia.scribe.flow.fetch_scribe_targets_task",
            new_callable=AsyncMock,
            return_value=[Video(id=str(i), title="input") for i in range(3)],
        ),
        patch(
            "maia.scribe.flow.process_transcript_task",
            new_callable=AsyncMock,
            side_effect=["transcript", "notice", "deferred"],
        ),
        patch("maia.scribe.flow.SCRIBE_THROTTLE_SECONDS", 0),
        patch("maia.scribe.flow.clear_quota_exhausted"),
    ):
        result = await scribe_operation()
    assert result["transcripts_staged"] == result["transcript_notices"] == 1
    assert result["videos_failed"] == 1 and result["videos_unavailable"] == 1
