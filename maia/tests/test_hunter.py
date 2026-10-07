"""
Tests for Maia Hunter module.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from maia.hunter.flow import (
    VideoIngestionBatchError,
    fetch_batch_task,
    ingest_results_task,
)


@pytest.fixture(autouse=True)
def mock_channel_enrichment():
    """Keep discovery unit tests hermetic; enrollment belongs to VideoRepository."""
    with patch("maia.hunter.flow.enrich_channels_task", new_callable=AsyncMock, return_value=0):
        yield


@pytest.fixture
def mock_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.search = AsyncMock()
    return strategy


@pytest.mark.asyncio
async def test_fetch_batch_empty_queue():
    """Test fetch_batch when queue is empty."""
    with patch("maia.hunter.flow.SearchQueueRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.fetch_batch = AsyncMock(return_value=[])

        result = await fetch_batch_task(batch_size=10)

        assert result == []
        mock_repo.fetch_batch.assert_called_once_with(10)


@pytest.mark.asyncio
async def test_fetch_batch_with_items(mock_search_queue_item: dict[str, Any]):
    """Test fetch_batch with items in queue."""
    from atlas.models import SearchQueueItem

    queue_item = SearchQueueItem(**mock_search_queue_item)

    with patch("maia.hunter.flow.SearchQueueRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.fetch_batch = AsyncMock(return_value=[queue_item])

        result = await fetch_batch_task(batch_size=10)

        assert len(result) == 1
        assert result[0]["query_term"] == "artificial intelligence"


@pytest.mark.asyncio
async def test_ingest_results_with_snowball(
    mock_strategy: MagicMock,
    mock_search_queue_item: dict[str, Any],
    mock_youtube_search_response: dict[str, Any],
):
    """Test ingest_results implements Snowball effect."""

    async def _passthrough_gate(items, *args, **kwargs):
        return items

    with (
        patch("maia.hunter.flow.VideoRepository") as MockVideoRepo,
        patch("maia.hunter.flow.SearchQueueRepository") as MockSearchRepo,
        patch("maia.hunter.flow.get_vault") as mock_get_vault,
        patch(
            "maia.hunter.flow.filter_by_quality",
            new=AsyncMock(side_effect=_passthrough_gate),
        ),
    ):
        mock_vault = mock_get_vault.return_value

        mock_video = MockVideoRepo.return_value
        mock_search = MockSearchRepo.return_value
        mock_video.ingest_video_metadata = AsyncMock()
        mock_search.add_terms = AsyncMock(return_value=3)
        mock_search.update_state = AsyncMock()
        mock_vault.store_metadata = MagicMock()

        await ingest_results_task(
            mock_search_queue_item, mock_youtube_search_response, mock_strategy
        )

        assert mock_video.ingest_video_metadata.call_count == 1

        mock_search.add_terms.assert_called_once()
        args = mock_search.add_terms.call_args[0][0]
        assert "test" in args
        assert "example" in args
        assert "ai" in args

        mock_search.update_state.assert_called_once()


@pytest.mark.asyncio
async def test_ingest_results_handles_vault_failure(
    mock_strategy: MagicMock,
    mock_search_queue_item: dict[str, Any],
    mock_youtube_search_response: dict[str, Any],
):
    """Test ingest_results continues even if vault storage fails."""

    async def _passthrough_gate(items, *args, **kwargs):
        return items

    with (
        patch("maia.hunter.flow.VideoRepository") as MockVideoRepo,
        patch("maia.hunter.flow.SearchQueueRepository") as MockSearchRepo,
        patch("maia.hunter.flow.get_vault") as mock_get_vault,
        patch(
            "maia.hunter.flow.filter_by_quality",
            new=AsyncMock(side_effect=_passthrough_gate),
        ),
    ):
        mock_vault = mock_get_vault.return_value

        mock_video = MockVideoRepo.return_value
        mock_search = MockSearchRepo.return_value
        mock_video.ingest_video_metadata = AsyncMock()
        mock_search.add_terms = AsyncMock(return_value=3)
        mock_search.update_state = AsyncMock()
        mock_vault.store_metadata = MagicMock(side_effect=Exception("Vault error"))

        await ingest_results_task(
            mock_search_queue_item, mock_youtube_search_response, mock_strategy
        )

        assert mock_video.ingest_video_metadata.call_count == 1


@pytest.mark.asyncio
async def test_ingest_results_none_response_returns_early(
    mock_search_queue_item: dict[str, Any],
):
    """Test ingest_results returns early on a None response without calling DAOs."""
    with (
        patch("maia.hunter.flow.VideoRepository") as MockVideoRepo,
        patch("maia.hunter.flow.SearchQueueRepository") as MockSearchRepo,
        patch("maia.hunter.flow.get_vault") as mock_get_vault,
    ):
        mock_video_repo = MockVideoRepo.return_value
        mock_search_repo = MockSearchRepo.return_value
        mock_video_repo.ingest_video_metadata = AsyncMock()
        mock_search_repo.add_terms = AsyncMock()

        await ingest_results_task(mock_search_queue_item, None, MagicMock())

        mock_video_repo.ingest_video_metadata.assert_not_called()
        mock_search_repo.add_terms.assert_not_called()
        mock_get_vault.assert_not_called()


@pytest.mark.asyncio
async def test_ingest_results_empty_tags_not_snowballed(
    mock_strategy: MagicMock,
    mock_search_queue_item: dict[str, Any],
):
    """Test ingest_results strips empty/whitespace tags before snowballing."""

    async def _passthrough_gate(items, *args, **kwargs):
        return items

    item = {
        "id": {"videoId": "test123"},
        "snippet": {
            "channelId": "UC123",
            "title": "Test",
            "tags": ["valid_tag", "", "   ", None, "ok_tag"],
        },
    }
    response = {"items": [item]}

    with (
        patch("maia.hunter.flow.VideoRepository") as MockVideoRepo,
        patch("maia.hunter.flow.SearchQueueRepository") as MockSearchRepo,
        patch("maia.hunter.flow.get_vault"),
        patch(
            "maia.hunter.flow.filter_by_quality",
            new=AsyncMock(side_effect=_passthrough_gate),
        ),
    ):
        mock_video = MockVideoRepo.return_value
        mock_search = MockSearchRepo.return_value
        mock_video.ingest_video_metadata = AsyncMock()
        mock_search.add_terms = AsyncMock(return_value=2)
        mock_search.update_state = AsyncMock()

        await ingest_results_task(mock_search_queue_item, response, mock_strategy)

        args = mock_search.add_terms.call_args[0][0]
        assert set(args) == {"valid_tag", "ok_tag"}
        mock_search.update_state.assert_awaited_once()


@pytest.mark.asyncio
async def test_ingest_results_replays_page_after_partial_db_failure(
    mock_strategy: MagicMock,
    mock_search_queue_item: dict[str, Any],
    mock_youtube_search_response: dict[str, Any],
):
    """A failed item blocks acknowledgement while successful items are attempted."""

    async def _passthrough_gate(items, *args, **kwargs):
        return items

    first = mock_youtube_search_response["items"][0]
    second = {**first, "id": {"kind": "youtube#video", "videoId": "FAIL0000001"}}
    third = {**first, "id": {"kind": "youtube#video", "videoId": "FAIL0000002"}}
    response = {
        "items": [first, second, third],
        "nextPageToken": "PAGE-2",
    }

    async def _ingest(item: dict[str, Any]) -> None:
        if item["id"]["videoId"].startswith("FAIL"):
            raise RuntimeError(item["id"]["videoId"])

    with (
        patch("maia.hunter.flow.VideoRepository") as MockVideoRepo,
        patch("maia.hunter.flow.SearchQueueRepository") as MockSearchRepo,
        patch("maia.hunter.flow.get_vault"),
        patch(
            "maia.hunter.flow.filter_by_quality",
            new=AsyncMock(side_effect=_passthrough_gate),
        ),
    ):
        mock_video = MockVideoRepo.return_value
        mock_search = MockSearchRepo.return_value
        mock_video.ingest_video_metadata = AsyncMock(side_effect=_ingest)
        mock_search.add_terms = AsyncMock(return_value=0)
        mock_search.update_state = AsyncMock()

        with pytest.raises(VideoIngestionBatchError) as raised:
            await ingest_results_task(mock_search_queue_item, response, mock_strategy)

        assert raised.value.failed_video_ids == ("FAIL0000001", "FAIL0000002")
        assert len(raised.value.failures) == 2
        assert mock_video.ingest_video_metadata.await_count == 3
        mock_search.add_terms.assert_not_awaited()
        mock_search.update_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_ingest_results_propagates_page_ack_failure(
    mock_strategy: MagicMock,
    mock_search_queue_item: dict[str, Any],
    mock_youtube_search_response: dict[str, Any],
):
    """A failed page-token write is a failed search, not a false acknowledgement."""

    async def _passthrough_gate(items, *args, **kwargs):
        return items

    with (
        patch("maia.hunter.flow.VideoRepository") as MockVideoRepo,
        patch("maia.hunter.flow.SearchQueueRepository") as MockSearchRepo,
        patch("maia.hunter.flow.get_vault"),
        patch(
            "maia.hunter.flow.filter_by_quality",
            new=AsyncMock(side_effect=_passthrough_gate),
        ),
    ):
        mock_video = MockVideoRepo.return_value
        mock_search = MockSearchRepo.return_value
        mock_video.ingest_video_metadata = AsyncMock()
        mock_search.add_terms = AsyncMock(return_value=0)
        mock_search.update_state = AsyncMock(side_effect=RuntimeError("ack write failed"))

        with pytest.raises(RuntimeError, match="ack write failed"):
            await ingest_results_task(
                mock_search_queue_item,
                mock_youtube_search_response,
                mock_strategy,
            )

        mock_video.ingest_video_metadata.assert_awaited_once()
        mock_search.update_state.assert_awaited_once()
