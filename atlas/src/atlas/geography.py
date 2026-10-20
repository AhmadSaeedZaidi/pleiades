"""Deterministic population-aware sampling; no network, SQL or inferred nationality."""

import hashlib
import json
import math
from functools import lru_cache
from importlib.resources import files


@lru_cache(maxsize=1)
def load_population() -> dict[str, int]:
    data = json.loads(files("atlas").joinpath("data/population_2024.json").read_text())
    populations = data["population"]
    if len(populations) < 200 or any(
        not isinstance(v, int) or v <= 0 for v in populations.values()
    ):
        raise ValueError("Invalid population snapshot")
    return dict(populations)


@lru_cache(maxsize=1)
def load_search_markets() -> tuple[str, ...]:
    data = json.loads(files("atlas").joinpath("data/population_2024.json").read_text())
    markets = data["search_markets"]
    if len(markets) < 80 or any(not isinstance(code, str) or len(code) != 2 for code in markets):
        raise ValueError("Invalid supported search markets")
    return tuple(markets)


def population_targets(
    populations: dict[str, int],
    exponent: float,
    cap: float,
    unknown_share: float,
    extra_countries: tuple[str, ...] = (),
) -> dict[str, float]:
    """Water-fill tempered population shares, preserving a separate unknown budget."""
    if not 0 <= exponent <= 1 or not 0 < cap <= 1 or not 0 <= unknown_share < 1:
        raise ValueError("Invalid geography target configuration")
    if not populations:
        raise ValueError("Population universe cannot be empty")
    if any(not isinstance(v, int) or v <= 0 for v in populations.values()):
        raise ValueError("Population values must be positive integers")
    weights = {country: value**exponent for country, value in populations.items()}
    # Missing population is not missing country identity. Use a neutral country
    # prior rather than fabricating a population or rejecting a supported market.
    neutral = sum(weights.values()) / len(weights)
    weights.update({code: neutral for code in extra_countries if code not in weights})
    if len(weights) * cap < 1 - unknown_share:
        raise ValueError("Country cap cannot cover the population universe")
    targets: dict[str, float] = {}
    remaining = 1 - unknown_share
    while weights:
        total = sum(weights.values())
        saturated = [
            country for country, value in weights.items() if remaining * value / total > cap
        ]
        if not saturated:
            targets.update(
                {country: remaining * value / total for country, value in weights.items()}
            )
            break
        for country in saturated:
            targets[country] = cap
            remaining -= cap
            del weights[country]
    return targets


class GeographyPolicy:
    """Apply the strongest lifetime/recent pressure and a recent hard concentration cap."""

    def __init__(
        self,
        targets: dict[str, float],
        lifetime: dict[str, int],
        recent: dict[str, int],
        *,
        cap: float,
        unknown_cap: float,
        penalty_weight: int = 30,
    ) -> None:
        self.targets = targets

        def normalize(counts: dict[str, int]) -> dict[str, int]:
            result: dict[str, int] = {}
            for country, count in counts.items():
                code = self.country(country)
                result[code] = result.get(code, 0) + count
            return result

        self.lifetime = normalize(lifetime)
        self.recent = normalize(recent)
        self.cap = cap
        self.unknown_cap = unknown_cap
        self.penalty_weight = penalty_weight

    def country(self, country: str | None) -> str:
        code = country.strip().upper() if isinstance(country, str) else ""
        return code if code in self.targets else "unknown"

    def target(self, country: str | None) -> float:
        return self.targets.get(self.country(country), self.unknown_cap)

    def pressure(self, country: str | None) -> float:
        code, target = self.country(country), self.target(country)
        if target <= 0:
            return math.inf
        # A hundred virtual balanced observations prevent empty-corpus lockout.
        return max(
            1.0,
            *(
                (counts.get(code, 0) + target * 100) / (sum(counts.values()) + 100) / target
                for counts in (self.lifetime, self.recent)
            ),
        )

    def probability(self, country: str | None) -> float:
        code = self.country(country)
        total = sum(self.recent.values())
        cap = self.unknown_cap if code == "unknown" else self.cap
        if total >= 100 and (self.recent.get(code, 0) + 1) / (total + 1) > cap:
            return 0.0
        return 1 / self.pressure(country)

    def penalty(self, country: str | None) -> int:
        pressure = self.pressure(country)
        if not math.isfinite(pressure):
            return -1000
        return -min(1000, math.ceil(self.penalty_weight * math.log2(pressure)))

    def admit(self, country: str | None, video_id: str) -> bool:
        # Stable across pages, retries and processes; no random retry lottery.
        draw = (
            int.from_bytes(
                hashlib.sha256(("geography-v1:" + video_id).encode()).digest()[:8], "big"
            )
            / 2**64
        )
        if draw >= self.probability(country):
            return False
        code = self.country(country)
        self.lifetime[code] = self.lifetime.get(code, 0) + 1
        self.recent[code] = self.recent.get(code, 0) + 1
        return True
