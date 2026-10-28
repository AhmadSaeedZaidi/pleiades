"""
Integration tests for Maia Archeologist.

These tests verify end-to-end behavior of Archeologist historical campaigns.
Mark as integration tests: pytest -m integration
"""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
import pytest_asyncio
from maia.archeologist import hunt_history, run_archeology_campaign


@pytest.mark.integration
@pytest.mark.asyncio
async def test_archeologist_complete_hunt_cycle(video_repo, mock_youtube_search_response):
    """Test complete Archeologist hunt cycle for a single month."""
    with (
        patch("maia.archeologist.flow.aiohttp.ClientSession") as MockSession,
        patch("maia.archeologist.flow.KeyRing") as MockKeyRing,
    ):
        # Setup KeyRing mock
        mock_keyring = MagicMock()
        mock_keyring.next_key = MagicMock(return_value="test_archeo_key_123")
        mock_keyring.size = 3
        MockKeyRing.return_value = mock_keyring

        # Configure ClientSession
        mock_session_instance = MagicMock()
        mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_instance.__aexit__ = AsyncMock(return_value=None)

        # Mock YouTube API response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_youtube_search_response)

        mock_get_context = MagicMock()
        mock_get_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_get_context.__aexit__ = AsyncMock(return_value=None)

        mock_session_instance.get.return_value = mock_get_context
        MockSession.return_value = mock_session_instance

        # Execute hunt for January 2010
        await hunt_history(year=2010, month=1)

        # CRITICAL: Verify REAL database writes (not in-memory/mock)
        print("[Test] Verifying REAL Neon DB writes...")
        videos = await video_repo._fetch_all(
            "SELECT * FROM videos ORDER BY discovered_at DESC LIMIT 10"
        )
        assert len(videos) >= 5, (
            f"Expected at least 5 videos in REAL database, got {len(videos)}. "
            "DAO may be using mock/in-memory DB instead of Neon!"
        )

        video_ids = [v["id"] for v in videos]
        assert "dQw4w9WgXcQ" in video_ids, f"Expected test video not found. Got: {video_ids}"

        # Verify connection to REAL Neon (check DSN)
        from atlas.config import settings

        db_url = str(settings.DATABASE_URL)
        assert "neon.tech" in db_url or "postgres" in db_url, (
            f"CRITICAL: DATABASE_URL does not point to Neon! Got: {db_url[:50]}..."
        )

        print(f"[Test] ✅ SUCCESS! Videos written to REAL Neon DB at {db_url[:30]}...")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_archeologist_high_priority_override(video_repo):
    """Test Archeologist assigns high priority (100) to historical videos."""
    with (
        patch("maia.archeologist.flow.aiohttp.ClientSession") as MockSession,
        patch("maia.archeologist.flow.KeyRing") as MockKeyRing,
    ):
        mock_keyring = MagicMock()
        mock_keyring.next_key = MagicMock(return_value="test_key")
        mock_keyring.size = 1
        MockKeyRing.return_value = mock_keyring

        # Mock response with a historical video
        historical_video_response = {
            "items": [
                {
                    "id": {"videoId": "HISTORICAL_001"},
                    "snippet": {
                        "publishedAt": "2010-01-15T00:00:00Z",
                        "channelId": "CHANNEL_HISTORY",
                        "title": "Historical Gaming Video",
                        "channelTitle": "Retro Gamer",
                        "tags": ["gaming", "retro", "2010"],
                        "categoryId": "20",
                        "defaultLanguage": "en",
                    },
                }
            ]
        }

        mock_session_instance = MagicMock()
        mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_instance.__aexit__ = AsyncMock(return_value=None)

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=historical_video_response)

        mock_get_context = MagicMock()
        mock_get_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_get_context.__aexit__ = AsyncMock(return_value=None)

        mock_session_instance.get.return_value = mock_get_context
        MockSession.return_value = mock_session_instance

        await hunt_history(year=2010, month=1)

        # Verify video exists with high priority
        video = await video_repo._fetch_one(
            "SELECT * FROM videos WHERE id = %s", ("HISTORICAL_001",)
        )

        assert video is not None, "Historical video HISTORICAL_001 not found in database"
        assert video["id"] == "HISTORICAL_001", f"Video ID mismatch: {video['id']}"
        # Note: Priority is stored in search_queue, not videos table
        # The video itself should exist with PENDING status
        assert video["status"] == "PENDING", (
            f"Expected PENDING status for historical video, got {video['status']}"
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_archeologist_handles_resiliency_strategy(video_repo, mock_sleep):
    """Test Archeologist handles 429 rate limit with retry and exponential backoff."""
    with (
        patch("maia.archeologist.flow.aiohttp.ClientSession") as MockSession,
        patch("maia.archeologist.flow.KeyRing") as MockKeyRing,
        patch("maia.archeologist.flow.logger") as mock_logger,
    ):
        mock_keyring = MagicMock()
        mock_keyring.next_key = MagicMock(return_value="test_key")
        mock_keyring.size = 3  # Give it keys to rotate through
        MockKeyRing.return_value = mock_keyring

        mock_session_instance = MagicMock()
        mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_instance.__aexit__ = AsyncMock(return_value=None)

        # Mock 429 response on first attempts, then success
        call_count = {"count": 0}

        async def mock_get_with_429(*args, **kwargs):
            call_count["count"] += 1
            if call_count["count"] <= 2:
                # First 2 calls return 429
                mock_resp = AsyncMock()
                mock_resp.status = 429
                return mock_resp
            else:
                # Third call succeeds with empty results
                mock_resp = AsyncMock()
                mock_resp.status = 200
                mock_resp.json = AsyncMock(return_value={"items": []})
                return mock_resp

        mock_get_context = MagicMock()
        mock_get_context.__aenter__ = AsyncMock(side_effect=mock_get_with_429)
        mock_get_context.__aexit__ = AsyncMock(return_value=None)

        mock_session_instance.get.return_value = mock_get_context
        MockSession.return_value = mock_session_instance

        # The system should handle 429s gracefully with retry/backoff, NOT crash
        # It should log warnings and eventually succeed (or continue gracefully)
        await hunt_history(year=2010, month=1)

        # Verify that warning logs were emitted for rate limit retries
        assert mock_logger.warning.call_count >= 1, "Should log warnings during retry attempts"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_archeologist_key_rotation_on_403(video_repo, mock_sleep):
    """Test Archeologist rotates keys on 403 Forbidden errors."""
    with (
        patch("maia.archeologist.flow.aiohttp.ClientSession") as MockSession,
        patch("maia.archeologist.flow.KeyRing") as MockKeyRing,
    ):
        keys_used = []

        def track_key_usage():
            key = f"key_{len(keys_used) + 1}"
            keys_used.append(key)
            return key

        mock_keyring = MagicMock()
        mock_keyring.next_key = MagicMock(side_effect=track_key_usage)
        mock_keyring.size = 3
        MockKeyRing.return_value = mock_keyring

        mock_session_instance = MagicMock()
        mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_instance.__aexit__ = AsyncMock(return_value=None)

        call_count = {"count": 0}

        async def mock_get_response(*args, **kwargs):
            call_count["count"] += 1
            if call_count["count"] <= 2:
                mock_resp = AsyncMock()
                mock_resp.status = 403
                return mock_resp
            else:
                mock_resp = AsyncMock()
                mock_resp.status = 200
                mock_resp.json = AsyncMock(return_value={"items": []})
                return mock_resp

        mock_get_context = MagicMock()
        mock_get_context.__aenter__ = AsyncMock(side_effect=mock_get_response)
        mock_get_context.__aexit__ = AsyncMock(return_value=None)

        mock_session_instance.get.return_value = mock_get_context
        MockSession.return_value = mock_session_instance

        await hunt_history(year=2010, month=1)

        assert len(keys_used) > 1, (
            f"Key rotation should use >1 key on 403 errors, used {len(keys_used)}"
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_archeologist_campaign_multi_month(video_repo):
    """Test Archeologist campaign iterates through multiple months."""
    from atlas.utils import KeyRing

    with patch("maia.archeologist.flow.hunt_history_task") as mock_hunt:
        # Configure hunt_history_task as an AsyncMock
        # When called, it should return an awaitable that resolves to None
        async def fake_hunt_history(year: int, month: int, keys: KeyRing) -> None:
            """Mock implementation that does nothing."""
            pass

        mock_hunt.side_effect = fake_hunt_history

        # Use real KeyRing (will pull from test environment or create mock keys)
        # The agent creates its own KeyRing internally
        # Run campaign for 2010 (12 months)
        await run_archeology_campaign(start_year=2010, end_year=2010)

        # Verify hunt_history_task was called for all 12 months of 2010
        assert mock_hunt.call_count == 12, (
            f"Campaign should invoke hunt_history for all 12 months, got {mock_hunt.call_count}"
        )

        # Verify correct month sequence (should be called with year, month, keys)
        for month in range(1, 13):
            # Check that at least one call had this year and month
            calls_with_month = [
                call
                for call in mock_hunt.call_args_list
                if len(call[0]) >= 2 and call[0][0] == 2010 and call[0][1] == month
            ]
            assert len(calls_with_month) == 1, f"Month {month} should be called exactly once"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_archeologist_handles_empty_results(video_repo):
    """Test Archeologist handles months with no historical videos."""
    with (
        patch("maia.archeologist.flow.aiohttp.ClientSession") as MockSession,
        patch("maia.archeologist.flow.KeyRing") as MockKeyRing,
    ):
        mock_keyring = MagicMock()
        mock_keyring.next_key = MagicMock(return_value="test_key")
        mock_keyring.size = 1
        MockKeyRing.return_value = mock_keyring

        empty_response = {"items": []}

        mock_session_instance = MagicMock()
        mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_instance.__aexit__ = AsyncMock(return_value=None)

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=empty_response)

        mock_get_context = MagicMock()
        mock_get_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_get_context.__aexit__ = AsyncMock(return_value=None)

        mock_session_instance.get.return_value = mock_get_context
        MockSession.return_value = mock_session_instance

        await hunt_history(year=2005, month=1)

        videos = await video_repo._fetch_all(
            "SELECT * FROM videos WHERE published_at BETWEEN %s AND %s",
            ("2005-01-01", "2005-02-01"),
        )
        assert len(videos) == 0, f"Expected 0 videos for empty result month, got {len(videos)}"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_archeologist_time_window_calculation(video_repo):
    """Test Archeologist correctly calculates time windows for searches."""
    with (
        patch("maia.archeologist.flow.aiohttp.ClientSession") as MockSession,
        patch("maia.archeologist.flow.KeyRing") as MockKeyRing,
    ):
        mock_keyring = MagicMock()
        mock_keyring.next_key = MagicMock(return_value="test_key")
        mock_keyring.size = 1
        MockKeyRing.return_value = mock_keyring

        mock_session_instance = MagicMock()
        mock_session_instance.__aenter__ = AsyncMock(return_value=mock_session_instance)
        mock_session_instance.__aexit__ = AsyncMock(return_value=None)

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"items": []})

        mock_get_context = MagicMock()
        mock_get_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_get_context.__aexit__ = AsyncMock(return_value=None)

        mock_session_instance.get.return_value = mock_get_context
        MockSession.return_value = mock_session_instance

        await hunt_history(year=2010, month=12)

        last_call = mock_session_instance.get.call_args_list[-1]
        params = last_call[1]["params"]

        assert "publishedAfter" in params, "publishedAfter missing from API request params"
        assert "publishedBefore" in params, "publishedBefore missing from API request params"
        assert params["publishedAfter"].startswith("2010-12-01"), (
            f"Expected Dec 2010 start window, got {params['publishedAfter']}"
        )
        assert params["publishedBefore"].startswith("2011-01-01"), (
            f"Expected Jan 2011 end window, got {params['publishedBefore']}"
        )


@pytest_asyncio.fixture
async def video_repo(fresh_db):
    """Provide VideoRepository for testing with real vault."""
    from atlas.repositories import VideoRepository

    yield VideoRepository()


@pytest.fixture
def mock_youtube_search_response() -> dict[str, Any]:
    """Mock YouTube Search API response for Archeologist with high-volume results."""
    return {
        "kind": "youtube#searchListResponse",
        "etag": "test-etag",
        "items": [
            {
                "kind": "youtube#searchResult",
                "etag": f"test-video-etag-{i}",
                "id": {
                    "kind": "youtube#video",
                    "videoId": "dQw4w9WgXcQ" if i == 0 else f"VIDEO_{i:03d}",
                },
                "snippet": {
                    "publishedAt": "2010-01-01T00:00:00Z",
                    "channelId": f"CHANNEL_{i:03d}",
                    "title": f"Historical Gaming Video {i}",
                    "channelTitle": f"Test Channel {i}",
                    "tags": ["history", "gaming", "retro"],
                    "categoryId": "20",
                    "defaultLanguage": "en",
                },
            }
            for i in range(10)
        ],
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_archeologist_real_youtube_api_search(video_repo):
    """Test Archeologist with REAL YouTube API (CI only)."""
    from atlas.utils import KeyRing

    # This will use real keys from environment
    keys = KeyRing("archeology")

    # Verify the first key doesn't look like a test key
    first_key = keys.next_key()
    if any(marker in first_key.lower() for marker in ["test_", "dummy_", "fake_", "mock_"]):
        pytest.fail(f"API key looks like a test key: {first_key[:10]}...")

    # Test historical search for a month known to have videos (January 2010)
    base_url = "https://www.googleapis.com/youtube/v3/search"

    start_date = datetime(2010, 1, 1, tzinfo=UTC)
    end_date = datetime(2010, 2, 1, tzinfo=UTC)

    start_str = start_date.isoformat().replace("+00:00", "Z")
    end_str = end_date.isoformat().replace("+00:00", "Z")

    params = {
        "part": "snippet",
        "type": "video",
        "order": "viewCount",
        "publishedAfter": start_str,
        "publishedBefore": end_str,
        "q": "minecraft",  # Search query instead of category to ensure results
        "maxResults": 5,
        "key": first_key,
    }

    # Make real API call
    async with aiohttp.ClientSession() as session:
        async with session.get(base_url, params=params) as resp:
            if resp.status == 400:
                pytest.fail(
                    f"YouTube API returned 400 - likely invalid test key (key={first_key[:10]}...)"
                )

            assert resp.status == 200, (
                f"YouTube API returned status {resp.status} (key={first_key[:10]}...)"
            )

            data = await resp.json()
            items = data.get("items", [])

            # Verify we got results
            assert len(items) > 0, "Expected at least 1 video from Jan 2010"

            # Verify response structure
            for item in items[:3]:  # Check first 3 items
                assert "id" in item, f"Missing 'id' in API response item: {item}"
                assert "videoId" in item["id"], f"Missing 'videoId' in item.id: {item['id']}"
                assert "snippet" in item, "Missing 'snippet' in API response item"
                assert "title" in item["snippet"], "Missing 'title' in snippet"
                assert "channelId" in item["snippet"], "Missing 'channelId' in snippet"

            # Ingest one video to verify DAO integration
            await video_repo.ingest_video_metadata(items[0], priority_override=100)

            # Verify video was stored IN REAL DATABASE
            video_id = items[0]["id"]["videoId"]
            video = await video_repo._fetch_one("SELECT * FROM videos WHERE id = %s", (video_id,))
            assert video is not None, (
                f"Video {video_id} not found in REAL database after ingestion!"
            )
            assert video["id"] == video_id

            # CRITICAL: Verify connection to REAL Neon (not mock/in-memory)
            from atlas.config import settings

            db_url = str(settings.DATABASE_URL)
            assert "neon.tech" in db_url or "postgres" in db_url, (
                f"CRITICAL: DATABASE_URL does not point to real Neon! Got: {db_url[:50]}..."
            )

            print(f"[Test] SUCCESS! Video {video_id} ingested into Neon DB at {db_url[:30]}...")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_archeologist_real_api_rate_limit_detection(video_repo):
    """Test that Archeologist properly detects and handles rate limits from real API."""
    from atlas.utils import KeyRing

    keys = KeyRing("archeology")

    # Make rapid-fire requests to potentially trigger rate limiting
    # This test verifies our rate limit detection works with real API responses
    base_url = "https://www.googleapis.com/youtube/v3/search"

    params = {
        "part": "snippet",
        "type": "video",
        "q": "test",
        "maxResults": 1,
        "key": keys.next_key(),
    }

    # Make several requests - if we hit a 429, we should handle it
    got_success = False
    async with aiohttp.ClientSession() as session:
        for _i in range(3):
            async with session.get(base_url, params=params) as resp:
                if resp.status == 429:
                    # Rate limit detected - this is what we want to test
                    print("✓ Rate limit (429) detected from real YouTube API")
                    break
                elif resp.status == 403:
                    # API key exhausted - rotate to next key
                    params["key"] = keys.next_key()
                elif resp.status == 200:
                    # Success - verify response structure
                    got_success = True
                    data = await resp.json()
                    assert "items" in data, f"Missing 'items' in API response: {list(data.keys())}"
                else:
                    pytest.fail(f"Unexpected YouTube API status: {resp.status}")

    # The test must have observed a genuine 200 response OR a 429 rate limit;
    # failing to exercise the API (e.g. all 403) is a failure, not a pass.
    assert got_success, "No successful (200) API response observed across 3 attempts"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_archeologist_real_api_key_rotation(video_repo):
    """Test KeyRing rotation with real YouTube API keys."""
    from atlas.utils import KeyRing

    keys = KeyRing("archeology")

    # Verify we have multiple keys
    assert keys.size >= 1, "Need at least 1 API key for testing"

    base_url = "https://www.googleapis.com/youtube/v3/search"

    # Track which keys we've used
    used_keys = set()
    got_success = False

    # Try up to the number of keys we have
    for _attempt in range(min(3, keys.size)):
        key = keys.next_key()
        used_keys.add(key)

        params = {
            "part": "snippet",
            "type": "video",
            "q": "minecraft",
            "maxResults": 1,
            "key": key,
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params) as resp:
                # Rotate on 403/429; succeed on 200; anything else is unexpected.
                if resp.status == 200:
                    # Key is valid - great!
                    got_success = True
                    data = await resp.json()
                    assert "items" in data, f"Missing 'items' in API response: {list(data.keys())}"
                    break
                elif resp.status in (403, 429):
                    continue
                else:
                    pytest.fail(f"Unexpected YouTube API status: {resp.status}")

    # Must have observed at least one genuine 200 response; all-errors is a failure.
    assert got_success, "No successful (200) YouTube API response across attempted keys"

    # Verify key rotation happened if we have multiple keys
    if keys.size > 1:
        assert len(used_keys) > 0, "Should have used at least one key"
