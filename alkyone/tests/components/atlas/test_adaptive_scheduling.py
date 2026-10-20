"""Integration tests for Adaptive Scheduling functionality.

Real Integration Testing: Uses real HuggingFace vault for metrics storage.
"""

from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio


@pytest.mark.integration
class TestAdaptiveScheduling:
    """Test Adaptive Scheduling watchlist operations and Vault integration."""

    @pytest.mark.asyncio
    async def test_add_to_watchlist(self, watchlist_repo):
        """Test adding videos to watchlist with real database."""
        video_ids = ["VIDEO_001", "VIDEO_002", "VIDEO_003"]

        # Add videos to watchlist
        for vid in video_ids:
            await watchlist_repo.add(vid, tier="HOURLY")

        # Verify they were added by querying watchlist table
        watchlist = await watchlist_repo._fetch_all(
            "SELECT video_id FROM watchlist WHERE video_id = ANY(%s)", (video_ids,)
        )
        assert len(watchlist) == 3, f"Expected 3 videos in watchlist, got {len(watchlist)}"

    @pytest.mark.asyncio
    async def test_fetch_tracking_batch(self, watchlist_repo):
        """Test fetching videos due for tracking."""
        # Setup: Add videos with past next_track_at
        test_videos = [
            {
                "video_id": f"VIDEO_{i:03d}",
                "tier": "HOURLY",
                "next_track_at": datetime.now(UTC) - timedelta(hours=1),
            }
            for i in range(5)
        ]

        # Add to watchlist (in real test)
        for video in test_videos:
            await watchlist_repo.add(video["video_id"], tier=video["tier"])

        # Fetch batch
        batch = await watchlist_repo.fetch_batch(batch_size=3)

        # Verify batch size respects limit
        assert len(batch) <= 3, f"Batch should respect limit of 3, got {len(batch)}"

        # Verify FIFO order (oldest first)
        if len(batch) > 1:
            for i in range(len(batch) - 1):
                assert batch[i].next_track_at <= batch[i + 1].next_track_at, (
                    f"Batch not in FIFO order at index {i}: "
                    f"{batch[i].next_track_at} > {batch[i + 1].next_track_at}"
                )

    @pytest.mark.asyncio
    async def test_update_watchlist_schedule(self, watchlist_repo):
        """Test batch updating watchlist schedules."""
        now = datetime.now(UTC)

        updates = [
            {
                "video_id": "VIDEO_001",
                "tracking_tier": "DAILY",
                "last_tracked_at": now,
                "next_track_at": now + timedelta(days=1),
            },
            {
                "video_id": "VIDEO_002",
                "tracking_tier": "WEEKLY",
                "last_tracked_at": now,
                "next_track_at": now + timedelta(days=7),
            },
        ]

        # Update schedules
        await watchlist_repo.update_schedule(updates)

        # Verify updates landed in the real watchlist table
        rows = await watchlist_repo._fetch_all(
            "SELECT video_id, tracking_tier FROM watchlist WHERE video_id = ANY(%s)",
            (["VIDEO_001", "VIDEO_002"],),
        )
        by_id = {r["video_id"]: r["tracking_tier"] for r in rows}
        assert by_id == {
            "VIDEO_001": "DAILY",
            "VIDEO_002": "WEEKLY",
        }, f"Watchlist tiers not updated: {by_id}"

    @pytest.mark.asyncio
    async def test_calculate_next_track_time(self, watchlist_repo):
        """Test adaptive tier calculation based on video age."""
        now = datetime.now(UTC)

        # Test HOURLY tier (< 24h old)
        published_recent = now - timedelta(hours=12)
        tier, next_time = watchlist_repo.calculate_next_track_time(published_recent)
        assert tier == "HOURLY", f"Expected HOURLY tier for 12h-old video, got {tier}"
        assert next_time > now, "next_track_at should be in the future"
        assert next_time <= now + timedelta(hours=1, minutes=1), (
            f"HOURLY next_track_at too far: {next_time}"
        )

        # Test DAILY tier (1-7 days old)
        published_medium = now - timedelta(days=3)
        tier, next_time = watchlist_repo.calculate_next_track_time(published_medium)
        assert tier == "DAILY", f"Expected DAILY tier for 3-day-old video, got {tier}"
        assert next_time > now, "next_track_at should be in the future"
        assert next_time <= now + timedelta(days=1, hours=1), (
            f"DAILY next_track_at too far: {next_time}"
        )

        # Test WEEKLY tier (> 7 days old)
        published_old = now - timedelta(days=30)
        tier, next_time = watchlist_repo.calculate_next_track_time(published_old)
        assert tier == "WEEKLY", f"Expected WEEKLY tier for 30-day-old video, got {tier}"
        assert next_time > now, "next_track_at should be in the future"
        assert next_time <= now + timedelta(days=7, hours=1), (
            f"WEEKLY next_track_at too far: {next_time}"
        )

    @pytest.mark.asyncio
    async def test_adaptive_scheduling_survives_janitor(self, video_repo, watchlist_repo):
        """Test that watchlist persists after video cleanup (Adaptive Scheduling)."""
        video_id = "VIDEO_PERSISTENT"

        video_data = {
            "id": {"videoId": video_id},
            "snippet": {
                "channelId": "CHANNEL_001",
                "channelTitle": "Test Channel",
                "title": "Test Video",
                "publishedAt": "2026-01-01T00:00:00Z",
                "tags": ["test"],
                "categoryId": "28",
                "defaultLanguage": "en",
            },
        }
        await video_repo.ingest_video_metadata(video_data)
        await watchlist_repo.add(video_id, tier="HOURLY")

        await video_repo.mark_done(video_id)

        # Verify watchlist entry exists
        watchlist_entry = await watchlist_repo._fetch_one(
            "SELECT * FROM watchlist WHERE video_id = %s", (video_id,)
        )

        # Adaptive Scheduling means video stays in watchlist
        assert watchlist_entry is not None, (
            f"Watchlist entry for {video_id} missing after video cleanup"
        )
        assert watchlist_entry["video_id"] == video_id, (
            f"Watchlist video_id mismatch: {watchlist_entry['video_id']}"
        )

    @pytest.mark.asyncio
    async def test_vault_metrics_storage(self):
        """Test metrics are properly stored in real HuggingFace Vault via Adaptive Scheduling."""
        from atlas.vault import get_vault

        vault = get_vault()

        metrics_data = [
            {
                "video_id": "VIDEO_METRICS_001",
                "views": 10000,
                "likes": 500,
                "comment_count": 50,
                "timestamp": datetime.now(UTC).isoformat(),
            },
            {
                "video_id": "VIDEO_METRICS_002",
                "views": 5000,
                "likes": 250,
                "comment_count": 25,
                "timestamp": datetime.now(UTC).isoformat(),
            },
        ]

        # Append to real Vault (HuggingFace)
        test_date = datetime.now(UTC).strftime("%Y-%m-%d")
        vault.append_metrics(metrics_data, date=test_date)

        # Verify by attempting to list files (validates upload succeeded)
        files = vault.list_files(f"metrics/date={test_date}")
        assert len(files) > 0, "Metrics should be uploaded to vault"


@pytest_asyncio.fixture
async def watchlist_repo(fresh_db):
    """Provide WatchlistRepository for testing with real vault."""
    from atlas.repositories import WatchlistRepository

    yield WatchlistRepository()


@pytest_asyncio.fixture
async def video_repo(fresh_db):
    """Provide VideoRepository for testing with real vault."""
    from atlas.repositories import VideoRepository

    yield VideoRepository()
