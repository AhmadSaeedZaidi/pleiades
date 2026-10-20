"""Geographic balancing cannot be bypassed by replay, absent metadata or huge tags."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from atlas.geography import GeographyPolicy
from maia.discovery import DiscoveryContext, balance_candidates, snowball_terms


def context():
    return DiscoveryContext(
        GeographyPolicy(
            {"IN": 0.05, "NG": 0.1},
            {"IN": 800, "NG": 0, "unknown": 200},
            {"IN": 800, "NG": 0, "unknown": 200},
            cap=0.1,
            unknown_cap=0.05,
        ),
        "NG",
    )


def item(key, country):
    return {
        "id": key,
        "snippet": {"channelId": "channel", "tags": ["Test", " test ", "science", "art", "extra"]},
        "_channel_country": country,
    }


async def test_replayed_videos_never_seed_and_concentrated_new_videos_are_held():
    c = context()
    with patch("maia.discovery.DiscoveryRepository") as repo:
        repo.return_value.existing_video_ids = AsyncMock(return_value={"old"})
        accepted = await balance_candidates(
            [item("old", "IN"), item("new", "IN"), item("fresh", "NG")], c, None
        )
    assert [i["id"] for i in accepted] == ["old", "fresh"]
    assert c.held == 1 and c.admitted == 1 and c.novel_ids == {"fresh"}
    assert c.feedback_penalty < 0
    assert sum(len(v) for v in snowball_terms(accepted, c).values()) == 3
    c.novel_ids.clear()
    assert snowball_terms(accepted, c) == {}


async def test_missing_country_lookup_failures_fail_closed():
    with (
        patch("maia.discovery.DiscoveryRepository") as repo,
        patch("maia.discovery.lookup_channels", new_callable=AsyncMock, side_effect=RuntimeError),
    ):
        repo.return_value.existing_video_ids = AsyncMock(return_value=set())
        with pytest.raises(RuntimeError):
            await balance_candidates(
                [{"id": "new", "snippet": {"channelId": "ch"}}], context(), MagicMock()
            )


async def test_quality_disabled_still_resolves_country_and_applies_cap():
    with (
        patch("maia.discovery.DiscoveryRepository") as repo,
        patch(
            "maia.discovery.lookup_channels",
            new_callable=AsyncMock,
            return_value=[{"id": "ch", "snippet": {"country": "IN"}}],
        ) as lookup,
    ):
        repo.return_value.existing_video_ids = AsyncMock(return_value=set())
        accepted = await balance_candidates(
            [{"id": "new", "snippet": {"channelId": "ch"}}], context(), MagicMock()
        )
    assert accepted == []
    lookup.assert_awaited_once()


def test_tag_growth_is_normalized_and_bounded_per_video_and_page():
    c = context()
    items = []
    for number in range(50):
        key = str(number)
        c.novel_ids.add(key)
        c.countries[key] = "NG"
        row = item(key, "NG")
        row["snippet"]["tags"] = [f"tag {number} {i}" for i in range(100)]
        items.append(row)
    groups = snowball_terms(items, c)
    assert sum(len(v) for v in groups.values()) == 20


async def test_search_uses_supported_market_without_reusing_old_pagination_token():
    from maia.hunter.flow import search_youtube_task

    strategy = MagicMock(search=AsyncMock(return_value={"items": []}))
    await search_youtube_task(
        {"query_term": "physics", "next_page_token": "old"}, strategy, context()
    )
    params = strategy.search.await_args.args[0]
    assert params["regionCode"] == "NG" and params["order"] == "relevance"
    assert "pageToken" not in params


@pytest.mark.parametrize("country,inserted", [("IN", True), ("NG", False)])
async def test_held_or_concurrently_existing_videos_cannot_reinforce_snowball(country, inserted):
    from maia.hunter.flow import ingest_results_task

    c = context()
    row = item("candidate", country)
    with (
        patch("maia.hunter.flow.filter_by_quality", new_callable=AsyncMock, return_value=[row]),
        patch("maia.discovery.DiscoveryRepository") as discovery_repo,
        patch("maia.hunter.flow.VideoRepository") as video_repo,
        patch("maia.hunter.flow.SearchQueueRepository") as queue_repo,
        patch("maia.hunter.flow.get_vault") as vault,
    ):
        discovery_repo.return_value.existing_video_ids = AsyncMock(return_value=set())
        video_repo.return_value.ingest_video_metadata = AsyncMock(return_value=inserted)
        queue_repo.return_value.set_geography_penalty = AsyncMock()
        queue_repo.return_value.add_terms = AsyncMock()
        queue_repo.return_value.update_state = AsyncMock()
        await ingest_results_task(
            {"id": 1, "query_term": "science"},
            {"items": [row], "nextPageToken": "legacy"},
            context=c,
        )
    queue_repo.return_value.add_terms.assert_not_awaited()
    queue_repo.return_value.update_state.assert_awaited_once_with(
        1, None, 0 if country == "IN" else 1, status="active"
    )
    if country == "IN":
        video_repo.return_value.ingest_video_metadata.assert_not_awaited()
        vault.return_value.store_batch.assert_not_called()
