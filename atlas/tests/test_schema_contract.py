"""Source-schema contracts for operational claim paths."""

from pathlib import Path


def _schema() -> str:
    return (Path(__file__).parents[1] / "src" / "atlas" / "schema.sql").read_text()


def test_streamer_partial_index_matches_newest_first_claim() -> None:
    schema = _schema()
    assert "idx_video_streamer_claim_v2 ON videos(discovered_at DESC)" in schema
    assert "WHERE status IN ('PENDING', 'PROCESSING') AND fetched = FALSE;" in schema


def test_singer_partial_index_covers_processed_self_heal_rows() -> None:
    schema = _schema()

    assert """CREATE INDEX IF NOT EXISTS idx_video_singer_claim_v2""" in schema
    assert (
        "WHERE status IN ('PENDING', 'PROCESSING', 'PROCESSED') "
        "AND fetched = TRUE AND has_audio = FALSE;"
    ) in schema


def test_claim_index_migration_is_concurrent_and_reversible() -> None:
    root = Path(__file__).parents[2]
    migration = (root / "deploy/sql/20260902_claim_indexes_v2.sql").read_text()
    rollback = (root / "deploy/sql/20260902_claim_indexes_v2_rollback.sql").read_text()

    for name in ("idx_video_streamer_claim_v2", "idx_video_singer_claim_v2"):
        assert f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {name}" in migration
        assert f"DROP INDEX CONCURRENTLY IF EXISTS {name}" in rollback

    assert "ANALYZE videos;" in migration


def test_watchlist_keeps_durable_tracker_sample_and_dormancy_state() -> None:
    schema = _schema()

    assert "'HOURLY', 'DAILY', 'WEEKLY', 'DORMANT'" in schema
    for column in (
        "last_views BIGINT",
        "last_likes BIGINT",
        "last_comment_count BIGINT",
        "unavailable_count INTEGER NOT NULL DEFAULT 0",
    ):
        assert column in schema


def test_tracker_migration_avoids_bulk_seed_and_covers_all_gap_rows() -> None:
    root = Path(__file__).parents[2]
    migration = (root / "deploy/sql/20260902_tracker_durable_state.sql").read_text()
    rollback = (root / "deploy/sql/20260902_tracker_durable_state_rollback.sql").read_text()

    assert "ALTER COLUMN tracking_tier SET NOT NULL" in migration
    assert "Bulk-seeding all" in migration
    assert "WITH latest_stats AS" not in migration
    assert "WHERE w.video_id IS NULL" in migration
    assert "idx_watchlist_due_active" not in migration + rollback
    assert "idx_watchlist_dormant_recheck" not in migration + rollback


def test_unsafe_generic_row_limit_helper_is_retired() -> None:
    root = Path(__file__).parents[2]
    migration = (root / "deploy/sql/20260902_retire_unsafe_row_limit.sql").read_text()

    assert "enforce_table_row_limit" not in _schema()
    assert "DROP FUNCTION IF EXISTS enforce_table_row_limit(REGCLASS, BIGINT)" in migration
