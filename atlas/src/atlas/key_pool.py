"""Dynamic API key-pool allocation driven by quota economics.

Splits the pool into hunting/tracking/archeology rings and caches the result to
a JSON override so agents read a stable value without querying the database.
"""

import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger("atlas.key_pool")

# Default location of the cached allocation, override with KEY_POOL_ALLOCATION_PATH.
DEFAULT_ALLOCATION_PATH = Path(
    os.environ.get(
        "KEY_POOL_ALLOCATION_PATH",
        str(Path(__file__).resolve().parents[3] / "data" / "pool_allocation.json"),
    )
)

# Recompute cadence: only refresh once the cache is older than this.
REFRESH_INTERVAL_DAYS = 7

# Quota economics (YouTube Data API v3): videos.list costs 1 unit (<=50 ids/call,
# the tracker's cheap stats refresh); search.list costs 1 unit but has its own
# 100-calls/day-per-key bucket, so the hunter is the inherent throttle point.
QUOTA_PER_KEY = 10_000
VIDEO_BATCH = 50
VIDEOS_LIST_UNIT_COST = 1
SEARCH_UNIT_COST = 1
SEARCH_BUCKET_PER_KEY = 100  # search.list calls/day per project/key

# Demand proxies (daily operations); the point is the RATIO driven by quota cost.
TRACKER_REFRESHES_PER_DAY = 4  # full-corpus stats refreshes / day
HUNTER_SEARCHES_PER_DAY = 1_500  # discovery searches / day


@dataclass(frozen=True)
class PoolSizes:
    """Resolved ring sizes for the current key pool."""

    tracking: int
    archeology: int


def tracker_demand_units(
    video_count: int, refreshes_per_day: int = TRACKER_REFRESHES_PER_DAY
) -> int:
    """Daily quota-unit demand for the tracking ring (batched videos.list)."""
    calls = refreshes_per_day * max(1, (video_count + VIDEO_BATCH - 1) // VIDEO_BATCH)
    return calls * VIDEOS_LIST_UNIT_COST


def compute_sizes(
    total_keys: int,
    video_count: int,
    archeology_size: int,
    hunter_searches_per_day: int = HUNTER_SEARCHES_PER_DAY,
) -> PoolSizes:
    """Compute ring sizes from quota math, balanced so no ring is starved.

    Keys are allocated proportional to each ring's daily quota demand, then
    clamped by safety floors; ``archeology`` is a fixed reserve and ``tracking``
    (the cheap videos.list ring) is never the first to exhaust.
    """
    # Archeology reserve — never starve it. Give it at least a meaningful share
    # (a third of the pool) so a few rate-limited keys can't fully block
    # historical hunting. archeology_size is a floor, not a ceiling.
    archeology_floor = max(archeology_size, max(2, total_keys // 3))
    archeology = min(archeology_floor, max(0, total_keys - 2))
    remaining = total_keys - archeology

    # Quota-math daily demand (units) for each ring.
    tracker_u = tracker_demand_units(video_count)
    hunter_u = hunter_searches_per_day * SEARCH_UNIT_COST
    total_u = tracker_u + hunter_u

    # Allocate keys proportional to quota demand...
    tracking = round(remaining * tracker_u / total_u)
    hunting = remaining - tracking

    # ...then apply safety floors so no ring is starved and the cheap tracker
    # ring is never the first to exhaust. Cap the tracker floor so it can't
    # swallow the whole pool on small key sets — tracking only does cheap
    # batched videos.list calls, so it needs far fewer keys than searchers.
    tracking_floor = min(6, max(2, (tracker_u + QUOTA_PER_KEY - 1) // QUOTA_PER_KEY + 4))
    hunting_floor = max(3, remaining // 3)
    if tracking < tracking_floor:
        tracking = tracking_floor
        hunting = remaining - tracking
    if hunting < hunting_floor:
        hunting = hunting_floor
        tracking = max(1, remaining - hunting)
    # Floors can't both fit in a tiny pool — give each at least one key.
    if tracking + hunting > remaining:
        tracking = max(1, remaining - 1)
        hunting = remaining - tracking

    return PoolSizes(tracking=tracking, archeology=archeology)


def load_override(path: Path = DEFAULT_ALLOCATION_PATH) -> PoolSizes | None:
    """Load cached ring sizes, or ``None`` if absent/invalid."""
    try:
        if not path.exists():
            return None
        data = json.loads(path.read_text())
        return PoolSizes(
            tracking=int(data["tracking"]),
            archeology=int(data["archeology"]),
        )
    except Exception as e:  # noqa: BLE001 - a bad cache must never break startup
        logger.warning(f"Ignoring invalid pool-allocation cache at {path}: {e}")
        return None


def _cache_age_days(path: Path) -> float | None:
    try:
        data = json.loads(path.read_text())
        computed_at = datetime.fromisoformat(data["computed_at"])
        return (datetime.now(UTC) - computed_at).total_seconds() / 86400.0
    except Exception:  # noqa: BLE001
        return None


def refresh_allocation(
    total_keys: int,
    video_count: int,
    archeology_size: int,
    path: Path = DEFAULT_ALLOCATION_PATH,
    force: bool = False,
) -> PoolSizes | None:
    """Recompute and cache ring sizes if the cache is stale (or ``force``).

    Returns the freshly-written :class:`PoolSizes`, or ``None`` if the cache was
    still fresh and nothing was written.
    """
    if not force:
        age = _cache_age_days(path)
        if age is not None and age < REFRESH_INTERVAL_DAYS:
            return None

    sizes = compute_sizes(total_keys, video_count, archeology_size)
    payload = {
        "tracking": sizes.tracking,
        "archeology": sizes.archeology,
        "video_count": video_count,
        "total_keys": total_keys,
        "computed_at": datetime.now(UTC).isoformat(),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))
    logger.info(
        f"Key-pool allocation refreshed: tracking={sizes.tracking}, "
        f"archeology={sizes.archeology}, hunting={total_keys - sizes.tracking - sizes.archeology} "
        f"(video_count={video_count})"
    )
    return sizes
