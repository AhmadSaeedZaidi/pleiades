"""
Pytest configuration and fixtures for Maia tests.
"""

import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from prefect.context import TaskRunContext

from unit_test_guard import isolate_unit_database as isolate_unit_database


@pytest.fixture(autouse=True)
def mock_prefect_context():
    """
    Establish a mock Prefect context so get_run_logger() works.

    We use TaskRunContext.model_construct() to bypass Pydantic validation
    and provide a lightweight context for all tests.
    """
    # Create dummy objects for the context
    mock_task_run = MagicMock()
    mock_task = MagicMock()
    mock_client = MagicMock()

    # Use model_construct to create the model without validation
    ctx = TaskRunContext.model_construct(
        task_run=mock_task_run,
        task=mock_task,
        client=mock_client,
    )

    # Enter the context manually
    token = TaskRunContext.__var__.set(ctx)
    yield
    # Reset the context
    TaskRunContext.__var__.reset(token)


@pytest.fixture(autouse=True)
def mock_sleep():
    """
    Mock asyncio.sleep to speed up tests.
    """
    with patch("asyncio.sleep", new_callable=AsyncMock) as mock:
        yield mock


@pytest.fixture(autouse=True)
def mock_prefect_logger():
    """
    Mock prefect.get_run_logger to return a standard logger.
    """
    with patch("prefect.get_run_logger") as mock:
        mock.return_value = logging.getLogger("test")
        yield mock


@pytest.fixture
def mock_youtube_search_response() -> dict[str, Any]:
    """Mock YouTube Search API response."""
    return {
        "kind": "youtube#searchListResponse",
        "etag": "test-etag",
        "nextPageToken": "NEXT_PAGE_TOKEN",
        "items": [
            {
                "kind": "youtube#searchResult",
                "etag": "test-video-etag",
                "id": {"kind": "youtube#video", "videoId": "dQw4w9WgXcQ"},
                "snippet": {
                    "publishedAt": "2023-01-01T00:00:00Z",
                    "channelId": "UCuAXFkgsw1L7xaCfnd5JJOw",
                    "title": "Test Video",
                    "channelTitle": "Test Channel",
                    "tags": ["test", "example", "ai"],
                    "categoryId": "28",
                    "defaultLanguage": "en",
                },
            }
        ],
    }


@pytest.fixture
def mock_youtube_stats_response() -> dict[str, Any]:
    """Mock YouTube Videos API statistics response."""
    return {
        "kind": "youtube#videoListResponse",
        "etag": "test-etag",
        "items": [
            {
                "kind": "youtube#video",
                "etag": "test-etag",
                "id": "dQw4w9WgXcQ",
                "statistics": {
                    "viewCount": "1000000",
                    "likeCount": "50000",
                    "commentCount": "1000",
                },
            }
        ],
    }


@pytest.fixture
def mock_search_queue_item() -> dict[str, Any]:
    """Mock search queue item from database."""
    return {
        "id": 1,
        "query_term": "artificial intelligence",
        "next_page_token": None,
        "last_searched_at": None,
        "priority": 5,
    }


@pytest.fixture
def mock_tracker_target() -> dict[str, Any]:
    """Mock tracker target video from database."""
    return {
        "id": "dQw4w9WgXcQ",
        "title": "Test Video",
        "published_at": "2023-01-01T00:00:00Z",
        "last_updated_at": None,
    }


@pytest.fixture
def mock_watchlist_item() -> dict[str, Any]:
    """Mock watchlist item returned by fetch_batch."""
    return {
        "video_id": "dQw4w9WgXcQ",
        "tracking_tier": "HOURLY",
        "last_tracked_at": None,
        "next_track_at": "2023-01-01T00:00:00Z",
        "created_at": "2023-01-01T00:00:00Z",
        "published_at": "2023-01-01T00:00:00Z",
    }
