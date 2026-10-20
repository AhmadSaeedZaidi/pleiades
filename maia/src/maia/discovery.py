"""Bounded discovery sampling and snowball feedback; metadata countries only."""

import hashlib
import math
import unicodedata
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from atlas.config import get_settings
from atlas.geography import (
    GeographyPolicy,
    load_population,
    load_search_markets,
    population_targets,
)
from atlas.repositories.discovery import DiscoveryRepository
from atlas.repositories.search_queue import SearchQueueRepository
from atlas.youtube import lookup_channels


@dataclass
class DiscoveryContext:
    policy: GeographyPolicy
    region: str
    novel_ids: set[str] = field(default_factory=set)
    countries: dict[str, str | None] = field(default_factory=dict)
    held: int = 0
    admitted: int = 0
    feedback_penalty: int = 0


def policy_from_snapshot(snapshot: dict[str, dict[str, int]]) -> GeographyPolicy:
    settings = get_settings()
    targets = population_targets(
        load_population(),
        settings.DISCOVERY_POPULATION_EXPONENT,
        settings.DISCOVERY_MAX_COUNTRY_SHARE,
        settings.DISCOVERY_UNKNOWN_COUNTRY_SHARE,
        extra_countries=load_search_markets(),
    )
    policy = GeographyPolicy(
        targets,
        snapshot["lifetime"],
        snapshot["recent"],
        cap=settings.DISCOVERY_MAX_COUNTRY_SHARE,
        unknown_cap=settings.DISCOVERY_UNKNOWN_COUNTRY_SHARE,
        penalty_weight=settings.DISCOVERY_PENALTY_WEIGHT,
    )
    return policy


async def collect_discovery_metrics() -> dict[str, Any]:
    snapshot = await DiscoveryRepository().geography_snapshot()
    policy = policy_from_snapshot(snapshot)
    settings = get_settings()
    total = sum(policy.recent.values())
    largest = max(policy.recent, key=lambda c: policy.recent[c], default="unknown")
    return {
        "enabled": settings.DISCOVERY_BALANCE_ENABLED,
        "population_exponent": settings.DISCOVERY_POPULATION_EXPONENT,
        "max_country_share": settings.DISCOVERY_MAX_COUNTRY_SHARE,
        "unknown_cap": settings.DISCOVERY_UNKNOWN_COUNTRY_SHARE,
        "recent_total": total,
        "largest_country": largest,
        "largest_share": policy.recent.get(largest, 0) / max(1, total),
        "largest_prior": policy.target(largest),
        "unknown_share": policy.recent.get("unknown", 0) / max(1, total),
    }


async def prepare_discovery() -> DiscoveryContext | None:
    settings = get_settings()
    if not settings.DISCOVERY_BALANCE_ENABLED:
        return None
    snapshot = await DiscoveryRepository().geography_snapshot()
    policy = policy_from_snapshot(snapshot)
    targets = policy.targets
    await SearchQueueRepository().ensure_seed_terms(settings.DISCOVERY_SEED_TERMS)
    # Population is a prior, not a supply estimate. Weighted deficit exploration
    # avoids spending every request on zero-yield markets with a zero count.
    slot = int(datetime.now(UTC).timestamp() // 1200)

    def market_key(country: str) -> float:
        observed = policy.recent.get(country, 0) / max(1, sum(policy.recent.values()))
        weight = max(targets[country] * 0.05, targets[country] - observed)
        digest = hashlib.sha256(f"{slot}:{country}".encode()).digest()
        draw = (int.from_bytes(digest[:8], "big") + 1) / (2**64 + 1)
        return -math.log(draw) / weight

    region = min((c for c in load_search_markets() if c in targets), key=market_key)
    return DiscoveryContext(policy, region)


async def balance_candidates(
    items: list[dict[str, Any]],
    context: DiscoveryContext,
    executor: Any,
) -> list[dict[str, Any]]:
    """Keep replays idempotent, resolve countries even when quality gating is off."""
    if len(items) > 50:
        raise ValueError("Discovery pages are bounded to 50 resources")
    context.novel_ids.clear()
    context.countries.clear()
    unique: dict[str, dict[str, Any]] = {}
    for item in items:
        key = item.get("id")
        if isinstance(key, dict):
            key = key.get("videoId")
        if isinstance(key, str) and key:
            unique[key] = item
    existing = await DiscoveryRepository().existing_video_ids(list(unique))
    missing = {
        item.get("snippet", {}).get("channelId")
        for item in unique.values()
        if "_channel_country" not in item
    } - {None}
    countries: dict[str, str | None] = {}
    if missing:
        if executor is None:
            raise RuntimeError("Country metadata requires a discovery executor")
        channels = await lookup_channels(sorted(missing), executor=executor, parts="snippet")
        countries = {
            channel["id"]: channel.get("snippet", {}).get("country") for channel in channels
        }
    passing: list[dict[str, Any]] = []
    penalties: list[int] = []
    for key, item in unique.items():
        country = item.get(
            "_channel_country", countries.get(item.get("snippet", {}).get("channelId"))
        )
        context.countries[key] = country
        penalties.append(context.policy.penalty(country))
        if key in existing:
            passing.append(item)
        elif context.policy.admit(country, key):
            passing.append(item)
            context.novel_ids.add(key)
            context.admitted += 1
        else:
            context.held += 1
    context.feedback_penalty = math.floor(sum(penalties) / len(penalties)) if penalties else 0
    return passing


def snowball_terms(items: list[dict[str, Any]], context: DiscoveryContext) -> dict[int, list[str]]:
    """Only new admitted IDs can reinforce terms; cap per-video and per-page growth."""
    settings = get_settings()
    groups: dict[int, list[str]] = {}
    seen: set[str] = set()
    seeds = {
        " ".join(unicodedata.normalize("NFKC", term).casefold().split())
        for term in settings.DISCOVERY_SEED_TERMS
    }
    for item in items:
        key = item.get("id")
        if isinstance(key, dict):
            key = key.get("videoId")
        if key not in context.novel_ids:
            continue
        penalty = context.policy.penalty(context.countries.get(key))
        tags = item.get("snippet", {}).get("tags", [])
        if not isinstance(tags, list):
            continue
        taken = 0
        for tag in tags:
            if not isinstance(tag, str):
                continue
            term = " ".join(unicodedata.normalize("NFKC", tag).casefold().split())
            if not 3 <= len(term) <= 80 or term in seen or term in seeds:
                continue
            if (
                taken >= settings.DISCOVERY_MAX_TAGS_PER_VIDEO
                or len(seen) >= settings.DISCOVERY_MAX_TERMS_PER_PAGE
            ):
                break
            seen.add(term)
            groups.setdefault(penalty, []).append(term)
            taken += 1
    return groups
