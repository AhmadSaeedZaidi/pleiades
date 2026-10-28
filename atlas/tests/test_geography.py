"""Population weighting must constrain concentration without guessing geography."""

import pytest
from atlas.geography import GeographyPolicy, population_targets


def test_tempered_population_targets_are_normalized_capped_and_not_country_specific():
    targets = population_targets({"AA": 10**9, "BB": 10**8, "CC": 10**6}, 0.5, 0.5, 0.05)
    assert sum(targets.values()) == pytest.approx(0.95)
    assert targets["AA"] == pytest.approx(0.5)
    assert targets["AA"] > targets["BB"] > targets["CC"]


def test_impossible_population_caps_fail_closed():
    with pytest.raises(ValueError):
        population_targets({"AA": 100}, 0.5, 0.1, 0.05)


def policy():
    return GeographyPolicy(
        {"IN": 0.05, "US": 0.1, "NG": 0.1},
        {"IN": 800, "US": 10, "NG": 0, "unknown": 190},
        {"IN": 800, "US": 10, "NG": 0, "unknown": 190},
        cap=0.1,
        unknown_cap=0.05,
    )


def test_overrepresented_country_is_held_and_has_negative_weight():
    p = policy()
    assert not p.admit("IN", "video")
    assert p.penalty("IN") < p.penalty("US") <= 0
    assert p.admit("NG", "new")
    assert not p.admit(None, "missing")


def test_admission_is_retry_stable_and_updates_page_budget():
    p = policy()
    assert p.probability("IN") == 0
    before = sum(p.recent.values())
    assert p.admit("NG", "one")
    assert sum(p.recent.values()) == before + 1
    assert p.probability("NG") <= 1


def test_unknown_country_is_not_assigned_from_language_or_title():
    p = GeographyPolicy({"US": 0.95}, {}, {}, cap=1, unknown_cap=0.05)
    assert p.country("en-IN") == p.country("not a country") == "unknown"
    assert p.country("us") == "US"


def test_population_snapshot_covers_large_and_small_countries():
    from atlas.geography import load_population

    populations = load_population()
    assert len(populations) > 200
    assert populations["IN"] > populations["US"] > populations["IS"] > 0


def test_sampling_is_reproducible_given_identical_observations():
    def draw():
        p = GeographyPolicy(
            {"AA": 0.1, "BB": 0.85},
            {"AA": 600, "BB": 400},
            {"AA": 600, "BB": 400},
            cap=1,
            unknown_cap=0.05,
        )
        return [p.admit("AA", str(i)) for i in range(20)]

    assert draw() == draw()
    assert any(draw()) and not all(draw())


def test_recent_country_cap_accounts_for_admissions_in_this_page():
    targets = {"AA": 0.1, **{f"B{i}": 0.85 / 9 for i in range(9)}}
    p = GeographyPolicy(
        targets, {"AA": 9, "B0": 91}, {"AA": 9, "B0": 91}, cap=0.1, unknown_cap=0.05
    )
    assert p.admit("AA", "first")
    assert not p.admit("AA", "second")


def test_unrecognized_metadata_is_counted_in_the_unknown_budget():
    p = GeographyPolicy(
        {"US": 0.95}, {"not-country": 100}, {"not-country": 100}, cap=1, unknown_cap=0.05
    )
    assert p.recent == {"unknown": 100}
    assert p.probability(None) == 0


def test_supported_market_with_missing_population_keeps_a_neutral_country_prior():
    targets = population_targets({"AA": 100, "BB": 100}, 0.5, 0.5, 0.05, extra_countries=("TW",))
    assert targets["TW"] == pytest.approx(targets["AA"])
    assert GeographyPolicy(targets, {}, {}, cap=0.5, unknown_cap=0.05).country("TW") == "TW"
