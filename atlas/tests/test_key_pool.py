"""Tests for dynamic key-pool allocation."""

import json

from atlas.key_pool import (
    PoolSizes,
    compute_sizes,
    load_override,
    refresh_allocation,
    tracker_demand_units,
)


def test_tracker_demand_units():
    """Tracker demand is cheap: 1 quota unit per batched videos.list call (<=50)."""
    # 4 refreshes/day of a 4595-video corpus -> 4 * ceil(4595/50) = 4*92 = 368 calls.
    assert tracker_demand_units(4595) == 368
    # 1000 videos -> 20 calls/refresh.
    assert tracker_demand_units(1000) == 80


def test_compute_sizes_small_corpus():
    """Small corpus: archeology gets a fair share, tracker protected, hunter keeps keys."""
    sizes = compute_sizes(total_keys=24, video_count=1_342, archeology_size=3)
    assert sizes.archeology == 8
    assert sizes.tracking == 5
    hunting = 24 - sizes.tracking - sizes.archeology
    assert hunting == 11


def test_compute_sizes_large_corpus_shifts_to_tracking():
    """Large corpus shifts more keys to tracking (its demand grows with corpus)."""
    sizes = compute_sizes(total_keys=24, video_count=600_000, archeology_size=3)
    assert sizes.archeology == 8
    assert sizes.tracking == 11
    assert 24 - sizes.tracking - sizes.archeology == 5


def test_compute_sizes_always_leaves_hunting_key():
    """Even with a tiny pool, at least one hunting key remains."""
    sizes = compute_sizes(total_keys=4, video_count=5_000_000, archeology_size=1)
    hunting = 4 - sizes.tracking - sizes.archeology
    assert hunting >= 1


def test_archeology_reserve_clamped():
    """Archeology reserve is floored at 1 (pool share) and cannot consume the whole pool."""
    sizes = compute_sizes(total_keys=3, video_count=0, archeology_size=10)
    assert 1 <= sizes.archeology <= 2


def test_refresh_writes_and_loads(tmp_path):
    """refresh_allocation writes a cache that load_override reads back."""
    path = tmp_path / "pool_allocation.json"
    sizes = refresh_allocation(24, 100_000, 3, path=path, force=True)
    assert sizes == PoolSizes(tracking=11, archeology=8)

    loaded = load_override(path)
    assert loaded == PoolSizes(tracking=11, archeology=8)

    payload = json.loads(path.read_text())
    assert payload["video_count"] == 100_000
    assert "computed_at" in payload


def test_refresh_respects_freshness(tmp_path):
    """A fresh cache is not rewritten unless forced."""
    path = tmp_path / "pool_allocation.json"
    refresh_allocation(24, 1_000, 3, path=path, force=True)
    # Not stale → returns None (no rewrite)
    assert refresh_allocation(24, 999_999, 3, path=path) is None
    # Cache still reflects the original computation (balanced, quota-math)
    assert load_override(path) == PoolSizes(tracking=5, archeology=8)


def test_load_override_missing_returns_none(tmp_path):
    assert load_override(tmp_path / "nope.json") is None


def test_load_override_invalid_returns_none(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("{ not valid json")
    assert load_override(path) is None
