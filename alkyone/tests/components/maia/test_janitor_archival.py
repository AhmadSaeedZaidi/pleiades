"""Integration tests for Janitor archival (Hot → Cold tier).

Real Integration Testing: Uses real HuggingFace vault storage for archival.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from atlas.models import VideoStats


@pytest.mark.integration
class TestJanitorArchival:
    """Test Janitor's stats archival from SQL to Vault."""

    # --- Helper Method ---
    async def _create_parent_videos(self, video_repo, video_ids: list[str]):
        """Helper to create parent video records to satisfy Foreign Key constraints."""
        for vid in video_ids:
            await video_repo.ingest_video_metadata(
                {
                    "id": {"videoId": vid},
                    "snippet": {
                        "channelId": "mock_channel",
                        "channelTitle": "Mock Channel",
                        "title": "Mock Video",
                        "publishedAt": datetime.now(UTC).isoformat(),
                        "tags": [],
                        "categoryId": "1",
                        "defaultLanguage": "en",
                    },
                }
            )

    @pytest.mark.asyncio
    async def test_archive_cold_stats_single_batch(self, video_repo):
        """Test archiving a single batch of old stats with real vault storage."""
        # Setup: Insert old stats into hot tier
        stats_data = [
            {
                "video_id": f"VIDEO_{i:03d}",
                "views": 1000 * i,
                "likes": 50 * i,
                "comment_count": 10 * i,
                "timestamp": datetime.now(UTC) - timedelta(days=10),
            }
            for i in range(50)
        ]

        video_ids = [s["video_id"] for s in stats_data]
        await self._create_parent_videos(video_repo, video_ids)
        await video_repo.log_stats_batch([VideoStats(**s) for s in stats_data])

        # Archive old stats (retention=7 days) with real vault
        archived_count = await video_repo.archive_cold_stats(retention_days=7, batch_size=5000)

        # Verify stats were archived
        assert archived_count == 50, f"Expected 50 stats archived, got {archived_count}"

        # Verify stats were removed from hot tier
        remaining = await video_repo._fetch_all(
            "SELECT * FROM video_stats_log WHERE timestamp < %s",
            (datetime.now(UTC) - timedelta(days=7),),
        )
        assert len(remaining) == 0, f"Expected 0 stats remaining in hot tier, got {len(remaining)}"

    @pytest.mark.asyncio
    async def test_archive_cold_stats_multiple_batches(self, video_repo):
        """Test archival loop drains large backlog in batches with real vault."""
        batch_size = 2000
        total_stats = 5000

        # Optimization: Reuse a small set of videos to avoid expensive video creation
        video_pool = [f"VIDEO_{i:03d}" for i in range(50)]
        await self._create_parent_videos(video_repo, video_pool)

        old_stats = [
            {
                "video_id": video_pool[i % len(video_pool)],  # Recycle video IDs
                "views": 1000,
                "likes": 50,
                "comment_count": 10,
                "timestamp": datetime.now(UTC) - timedelta(days=8, hours=i % 24),
            }
            for i in range(total_stats)
        ]

        await video_repo.log_stats_batch([VideoStats(**s) for s in old_stats])

        # Run archival loop with real vault
        total_archived = 0
        iterations = 0
        while iterations < 5:
            archived = await video_repo.archive_cold_stats(retention_days=7, batch_size=batch_size)
            if archived == 0:
                break
            total_archived += archived
            iterations += 1

        assert total_archived == total_stats, (
            f"Expected {total_stats} total archived, got {total_archived}"
        )
        assert iterations == 3, (
            f"Expected 3 archival iterations for {total_stats} stats"
            f" at batch_size={batch_size}, got {iterations}"
        )

        # Verify stats were removed from hot tier
        remaining = await video_repo._fetch_all(
            "SELECT * FROM video_stats_log WHERE timestamp < %s",
            (datetime.now(UTC) - timedelta(days=7),),
        )
        assert len(remaining) == 0, (
            f"Expected 0 stats remaining in hot tier after full archival, got {len(remaining)}"
        )

    @pytest.mark.asyncio
    async def test_archive_respects_retention_period(self, video_repo):
        """Test that only stats older than retention period are archived with real vault."""
        now = datetime.now(UTC)

        # Create stats lists
        old_stats = [
            {
                "video_id": f"OLD_{i:03d}",
                "views": 1000,
                "likes": 50,
                "comment_count": 10,
                "timestamp": now - timedelta(days=10),
            }
            for i in range(50)
        ]
        new_stats = [
            {
                "video_id": f"NEW_{i:03d}",
                "views": 500,
                "likes": 25,
                "comment_count": 5,
                "timestamp": now - timedelta(days=3),
            }
            for i in range(50)
        ]
        stats = old_stats + new_stats

        video_ids = [s["video_id"] for s in stats]
        await self._create_parent_videos(video_repo, video_ids)
        await video_repo.log_stats_batch([VideoStats(**s) for s in stats])

        # Archive with 7-day retention to real vault
        archived = await video_repo.archive_cold_stats(retention_days=7)

        # Only old stats should be archived
        assert archived == 50, f"Expected 50 old stats archived (retention=7d), got {archived}"

        # Verify new stats remain in hot tier
        remaining = await video_repo._fetch_all(
            "SELECT * FROM video_stats_log WHERE timestamp >= %s",
            (now - timedelta(days=7),),
        )
        assert len(remaining) == 50, (
            f"Expected 50 new stats to remain in hot tier, got {len(remaining)}"
        )

    @pytest.mark.asyncio
    async def test_vault_failure_prevents_deletion(self, video_repo):
        """Test transactional safety: don't delete if Vault upload fails."""
        old_stats = [
            {
                "video_id": "VIDEO_001",
                "views": 1000,
                "likes": 50,
                "comment_count": 10,
                "timestamp": datetime.now(UTC) - timedelta(days=10),
            }
        ]

        await self._create_parent_videos(video_repo, ["VIDEO_001"])
        await video_repo.log_stats_batch([VideoStats(**old_stats[0])])

        # Mock get_vault() to return a vault whose append_metrics always fails
        with patch("atlas.vault.get_vault") as mock_get_vault:
            mock_vault = MagicMock()
            mock_vault.append_metrics = MagicMock(side_effect=Exception("Simulated Vault failure"))
            mock_get_vault.return_value = mock_vault

            # Attempt archival (Vault will fail)
            with pytest.raises(Exception, match="Simulated Vault failure"):
                await video_repo.archive_cold_stats(retention_days=7)

        # Verify stats were NOT deleted from hot tier (transaction rollback)
        remaining = await video_repo._fetch_all(
            "SELECT * FROM video_stats_log WHERE video_id = %s", ("VIDEO_001",)
        )
        assert len(remaining) == 1, "Stats should remain after vault failure"

    @pytest.mark.asyncio
    async def test_archival_groups_by_date(self, video_repo):
        """Test that stats are grouped by date for efficient Parquet storage with real vault."""
        stats = []
        # Create stats across 3 days
        for day_offset in range(10, 13):
            for i in range(10):
                stats.append(
                    {
                        "video_id": f"VIDEO_{i:03d}",
                        "views": 1000,
                        "likes": 50,
                        "comment_count": 10,
                        "timestamp": datetime.now(UTC) - timedelta(days=day_offset, hours=i),
                    }
                )

        video_ids = list(set(s["video_id"] for s in stats))
        await self._create_parent_videos(video_repo, video_ids)
        await video_repo.log_stats_batch([VideoStats(**s) for s in stats])

        # Archive to real vault (groups by date internally)
        archived_count = await video_repo.archive_cold_stats(retention_days=7)

        # Verify all stats were archived
        assert archived_count == 30, (
            f"Expected 30 stats archived across 3 days, got {archived_count}"
        )

        # Verify stats were removed from hot tier
        remaining = await video_repo._fetch_all(
            "SELECT * FROM video_stats_log WHERE timestamp < %s",
            (datetime.now(UTC) - timedelta(days=7),),
        )
        assert len(remaining) == 0, (
            f"Expected 0 stats remaining after date-grouped archival, got {len(remaining)}"
        )

    @pytest.mark.asyncio
    async def test_janitor_full_cycle(self, video_repo, monkeypatch):
        """Test complete Janitor cycle with real vault: archive stats + cleanup videos."""
        # Enable janitor for this test
        from maia.janitor.flow import janitor_cycle

        from atlas import settings

        monkeypatch.setattr(settings, "JANITOR_ENABLED", True)
        monkeypatch.setattr(settings, "JANITOR_RETENTION_DAYS", 7)
        monkeypatch.setattr(settings, "JANITOR_SAFETY_CHECK", True)

        # Setup: Create old video with stats
        video_id = "VIDEO_JANITOR_TEST"

        # 1. Ingest video
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

        # 2. Add stats
        stats_data = [
            {
                "video_id": video_id,
                "views": 10000,
                "likes": 500,
                "comment_count": 50,
                "timestamp": datetime.now(UTC) - timedelta(days=10),
            }
        ]
        await video_repo.log_stats_batch([VideoStats(**s) for s in stats_data])

        # 3. Mark video as done (eligible for cleanup)
        await video_repo.mark_transcript_safe(video_id)
        await video_repo.mark_done(video_id)

        # Override discovered_at to be old enough for cleanup (default retention is 7 days)
        old_date = datetime.now(UTC) - timedelta(days=10)
        query = "UPDATE videos SET discovered_at = %s WHERE id = %s"
        await video_repo._execute(query, (old_date, video_id))

        # 4. Run Janitor cycle with real vault
        result = await janitor_cycle(dry_run=False, archive_stats=True)

        # 5. Verify results
        assert result["stats_archived"] >= 1, (
            f"Expected >=1 stats archived to vault, got {result['stats_archived']}"
        )
        assert result["cleanup_stats"]["deleted"] >= 1, (
            f"Expected >=1 video cleaned up, got {result['cleanup_stats']['deleted']}"
        )

    @pytest.mark.asyncio
    async def test_archival_performance_large_dataset(self, video_repo):
        """Performance test: Archive 10k stats to real vault in reasonable time."""
        import time

        total_stats = 10000
        batch_size = 5000

        # Optimization: Reuse a small set of videos
        video_pool = [f"VIDEO_{i:03d}" for i in range(50)]
        await self._create_parent_videos(video_repo, video_pool)

        for batch_start in range(0, total_stats, batch_size):
            batch_stats = [
                {
                    "video_id": video_pool[i % len(video_pool)],  # Recycle IDs
                    "views": 1000,
                    "likes": 50,
                    "comment_count": 10,
                    "timestamp": datetime.now(UTC) - timedelta(days=10, minutes=i),
                }
                for i in range(batch_start, min(batch_start + batch_size, total_stats))
            ]
            await video_repo.log_stats_batch([VideoStats(**s) for s in batch_stats])

        # Measure archival time to real vault
        start_time = time.time()

        total_archived = 0
        while True:
            archived = await video_repo.archive_cold_stats(retention_days=7, batch_size=3000)
            if archived == 0:
                break
            total_archived += archived

        elapsed = time.time() - start_time

        assert total_archived == total_stats, (
            f"Expected {total_stats} stats archived in perf test, got {total_archived}"
        )
        # Real-infra budget: Neon DB roundtrips + HuggingFace API uploads.
        # 10k rows / 4 batch uploads typically completes in 60-90s.
        assert elapsed < 180, f"Archival took {elapsed}s, expected < 180s"


@pytest_asyncio.fixture
async def video_repo(fresh_db):
    """Provide VideoRepository for testing with real vault."""
    from atlas.repositories import VideoRepository

    yield VideoRepository()
