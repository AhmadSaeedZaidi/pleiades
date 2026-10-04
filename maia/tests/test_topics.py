import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from maia.topics import topics_operation


@pytest.mark.asyncio
async def test_enrichment_is_bounded_fair_and_uses_hunting_capacity():
    repo = SimpleNamespace(
        claim=AsyncMock(side_effect=[("t1", ["v"]), ("t2", ["c"])]),
        complete=AsyncMock(return_value={"observed": 1, "unavailable": 0, "deferred": 0}),
        release=AsyncMock(),
    )
    with (
        patch("maia.topics.TopicRepository", return_value=repo),
        patch("maia.topics.get_settings", return_value=SimpleNamespace(TOPIC_SYNC_ENABLED=True)),
        patch("maia.topics.lookup_videos", AsyncMock(return_value=[{"id": "v"}])) as videos,
        patch("maia.topics.lookup_channels", AsyncMock(return_value=[{"id": "c"}])) as channels,
    ):
        result = await topics_operation(max_batches=2)
    assert result == {"observed": 2, "unavailable": 0, "deferred": 0, "batches": 2}
    assert [call.args for call in repo.claim.await_args_list] == [("video", 50), ("channel", 50)]
    videos.assert_awaited_once_with(["v"], parts="topicDetails", key_ring_pool="hunting")
    channels.assert_awaited_once_with(["c"], parts="topicDetails", key_ring_pool="hunting")


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeError("outage"), asyncio.CancelledError()])
async def test_failed_or_cancelled_api_request_releases_owned_lease(error):
    repo = SimpleNamespace(
        claim=AsyncMock(return_value=("token", ["v"])), complete=AsyncMock(), release=AsyncMock()
    )
    with (
        patch("maia.topics.TopicRepository", return_value=repo),
        patch("maia.topics.get_settings", return_value=SimpleNamespace(TOPIC_SYNC_ENABLED=True)),
        patch("maia.topics.lookup_videos", AsyncMock(side_effect=error)),
    ):
        with pytest.raises(type(error)):
            await topics_operation(max_batches=1)
    repo.release.assert_awaited_once_with("video", "token")
    repo.complete.assert_not_called()


@pytest.mark.asyncio
async def test_disabled_enrichment_never_constructs_repository():
    with (
        patch("maia.topics.get_settings", return_value=SimpleNamespace(TOPIC_SYNC_ENABLED=False)),
        patch("maia.topics.TopicRepository") as factory,
    ):
        assert (await topics_operation())["batches"] == 0
    factory.assert_not_called()
