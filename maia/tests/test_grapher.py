import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from maia.grapher.flow import grapher_operation


@pytest.mark.asyncio
async def test_registered_agent_accepts_cli_arguments():
    import argparse

    from maia.registry import AGENT_REGISTRY

    agent_class = AGENT_REGISTRY["grapher"]
    parser = argparse.ArgumentParser()
    agent_class.add_cli_args(parser)
    args = parser.parse_args(["--batch-size", "10", "--max-batches", "2"])
    with patch(
        "maia.grapher.flow.grapher_operation", new_callable=AsyncMock, return_value={"batches": 2}
    ) as operation:
        assert await agent_class().run(**vars(args)) == {"batches": 2}
    operation.assert_awaited_once_with(batch_size=10, max_batches=2)


@pytest.mark.asyncio
async def test_missing_dedicated_key_does_not_claim_or_borrow_hunting_capacity():
    with (
        patch(
            "maia.grapher.flow.get_settings", return_value=SimpleNamespace(TOPIC_SYNC_ENABLED=True)
        ),
        patch("maia.grapher.flow.KeyRing", side_effect=ValueError("Empty KeyRing for grapher")),
        patch("maia.grapher.flow.TopicRepository") as factory,
    ):
        with pytest.raises(ValueError, match="Empty KeyRing"):
            await grapher_operation()
    factory.assert_not_called()


@pytest.mark.asyncio
async def test_enrichment_is_bounded_fair_and_uses_dedicated_grapher_capacity():
    repo = SimpleNamespace(
        claim=AsyncMock(side_effect=[("t1", ["v"]), ("t2", ["c"])]),
        complete=AsyncMock(return_value={"observed": 1, "unavailable": 0, "deferred": 0}),
        release=AsyncMock(),
    )
    executor = SimpleNamespace()
    with (
        patch("maia.grapher.flow.KeyRing", return_value=SimpleNamespace()) as ring,
        patch("maia.grapher.flow.ResiliencyExecutor", return_value=executor) as executor_factory,
        patch("maia.grapher.flow.TopicRepository", return_value=repo),
        patch(
            "maia.grapher.flow.get_settings", return_value=SimpleNamespace(TOPIC_SYNC_ENABLED=True)
        ),
        patch("maia.grapher.flow.lookup_videos", AsyncMock(return_value=[{"id": "v"}])) as videos,
        patch(
            "maia.grapher.flow.lookup_channels", AsyncMock(return_value=[{"id": "c"}])
        ) as channels,
    ):
        result = await grapher_operation(max_batches=2)
    ring.assert_called_once_with("grapher")
    executor_factory.assert_called_once_with(ring.return_value, agent_name="grapher")
    assert result == {"observed": 2, "unavailable": 0, "deferred": 0, "batches": 2}
    assert [call.args for call in repo.claim.await_args_list] == [("video", 50), ("channel", 50)]
    videos.assert_awaited_once_with(["v"], parts="topicDetails", executor=executor)
    channels.assert_awaited_once_with(["c"], parts="topicDetails", executor=executor)


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeError("outage"), asyncio.CancelledError()])
async def test_failed_or_cancelled_api_request_releases_owned_lease(error):
    repo = SimpleNamespace(
        claim=AsyncMock(return_value=("token", ["v"])), complete=AsyncMock(), release=AsyncMock()
    )
    with (
        patch("maia.grapher.flow.TopicRepository", return_value=repo),
        patch("maia.grapher.flow.KeyRing"),
        patch("maia.grapher.flow.ResiliencyExecutor"),
        patch(
            "maia.grapher.flow.get_settings", return_value=SimpleNamespace(TOPIC_SYNC_ENABLED=True)
        ),
        patch("maia.grapher.flow.lookup_videos", AsyncMock(side_effect=error)),
    ):
        with pytest.raises(type(error)):
            await grapher_operation(max_batches=1)
    repo.release.assert_awaited_once_with("video", "token")
    repo.complete.assert_not_called()


@pytest.mark.asyncio
async def test_disabled_enrichment_never_constructs_repository():
    with (
        patch(
            "maia.grapher.flow.get_settings", return_value=SimpleNamespace(TOPIC_SYNC_ENABLED=False)
        ),
        patch("maia.grapher.flow.TopicRepository") as factory,
    ):
        assert (await grapher_operation())["batches"] == 0
    factory.assert_not_called()
