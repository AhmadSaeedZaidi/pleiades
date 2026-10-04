"""
Tests for Maia Archeologist module.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from atlas.utils import QuotaExhaustedError
from maia.archeologist.flow import archeology_operation, hunt_history_task


@pytest.fixture
def mock_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.search = AsyncMock()
    return strategy


@pytest.mark.asyncio
async def test_hunt_history_successful_retrieval(mock_strategy: MagicMock):
    """Test hunt_history successfully retrieves and ingests historical videos."""
    mock_response_data: dict[str, Any] = {
        "kind": "youtube#searchListResponse",
        "items": [
            {
                "id": {"videoId": "OLD_VIDEO_001"},
                "snippet": {
                    "publishedAt": "2010-05-15T00:00:00Z",
                    "channelId": "CHANNEL_HISTORY_001",
                    "title": "Historical Video",
                    "channelTitle": "Historical Channel",
                    "tags": ["history", "gaming"],
                    "categoryId": "20",
                    "defaultLanguage": "en",
                },
            },
            {
                "id": {"videoId": "OLD_VIDEO_002"},
                "snippet": {
                    "publishedAt": "2010-05-20T00:00:00Z",
                    "channelId": "CHANNEL_HISTORY_002",
                    "title": "Another Old Video",
                    "channelTitle": "Vintage Channel",
                    "tags": ["retro"],
                    "categoryId": "20",
                    "defaultLanguage": "en",
                },
            },
        ],
    }
    mock_strategy.search.return_value = mock_response_data

    async def _passthrough_gate(items, *args, **kwargs):
        return items

    with (
        patch("maia.archeologist.flow.VideoRepository") as MockRepo,
        patch("maia.archeologist.flow.enrich_channels_task", new_callable=AsyncMock) as mock_enrich,
        patch(
            "maia.archeologist.flow.filter_by_quality",
            new=AsyncMock(side_effect=_passthrough_gate),
        ),
    ):
        mock_repo = MockRepo.return_value
        mock_repo.ingest_video_metadata = AsyncMock()
        mock_enrich.return_value = 0

        await hunt_history_task(year=2010, month=5, strategy=mock_strategy)

        assert mock_repo.ingest_video_metadata.call_count == 10
        mock_strategy.search.assert_called()


@pytest.mark.asyncio
async def test_hunt_history_handles_429_resiliency_strategy(mock_strategy: MagicMock):
    """Test Archeologist raises QuotaExhaustedError on all-keys-exhausted."""
    mock_strategy.search.side_effect = QuotaExhaustedError("All keys exhausted")

    with patch("maia.archeologist.flow.VideoRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.ingest_video_metadata = AsyncMock()

        with pytest.raises(QuotaExhaustedError):
            await hunt_history_task(year=2010, month=1, strategy=mock_strategy)


@pytest.mark.asyncio
async def test_hunt_history_handles_api_errors(mock_strategy: MagicMock):
    """Test Archeologist handles API errors gracefully."""
    mock_strategy.search.side_effect = Exception("API error")

    with patch("maia.archeologist.flow.VideoRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.ingest_video_metadata = AsyncMock()

        await hunt_history_task(year=2010, month=1, strategy=mock_strategy)

        assert mock_repo.ingest_video_metadata.call_count == 0


@pytest.mark.asyncio
async def test_hunt_history_handles_empty_response(mock_strategy: MagicMock):
    """Test hunt_history handles empty API responses gracefully."""
    mock_strategy.search.return_value = {"kind": "youtube#searchListResponse", "items": []}

    with patch("maia.archeologist.flow.VideoRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.ingest_video_metadata = AsyncMock()

        await hunt_history_task(year=2025, month=1, strategy=mock_strategy)

        assert mock_repo.ingest_video_metadata.call_count == 0


@pytest.mark.asyncio
async def test_run_archeology_campaign_iterates_through_years(mock_strategy: MagicMock):
    """Test archeology campaign iterates through multiple years and months."""
    with patch(
        "maia.archeologist.flow.hunt_history_task", new_callable=AsyncMock
    ) as mock_hunt_task:
        result = await archeology_operation(start_year=2010, end_year=2011, strategy=mock_strategy)

        assert mock_hunt_task.call_count == 24

        first_call = mock_hunt_task.call_args_list[0]
        assert first_call[0] == (2010, 1, mock_strategy)

        last_call = mock_hunt_task.call_args_list[-1]
        assert last_call[0] == (2011, 12, mock_strategy)

        assert result["years_processed"] == 2
        assert result["months_processed"] == 24
