"""Tests for the on-disk rate-limit back-off state in :mod:`atlas.state`."""

import time

import atlas.state as state


def test_bump_grows_exponentially_and_caps(monkeypatch, tmp_path):
    monkeypatch.setattr(state, "_STATE_PATH", tmp_path / "agent_state.json")

    assert state.get_rate_limit_cooldown_until("streamer") is None

    t0 = time.time()
    cds = [state.bump_rate_limit_cooldown("streamer") for _ in range(6)]
    # Durations: 300, 600, 1200, 2400, then capped at 3600.
    expected = [300, 600, 1200, 2400, 3600, 3600]
    for cd, exp in zip(cds, expected, strict=True):
        assert abs((cd - t0) - exp) <= 2
    # Capped: the 5th and 6th attempts both schedule a 3600s backoff, so the
    # absolute timestamps differ only by the (sub-second) inter-call elapsed time.
    assert cds[5] - cds[4] < 5


def test_clear_resets_cooldown(monkeypatch, tmp_path):
    monkeypatch.setattr(state, "_STATE_PATH", tmp_path / "agent_state.json")

    state.bump_rate_limit_cooldown("streamer")
    assert state.is_rate_limited("streamer") is True

    state.clear_rate_limit_cooldown("streamer")
    assert state.is_rate_limited("streamer") is False
    assert state.get_rate_limit_cooldown_until("streamer") is None


def test_is_rate_limited_ignores_expired(monkeypatch, tmp_path):
    monkeypatch.setattr(state, "_STATE_PATH", tmp_path / "agent_state.json")

    state._write({"rate_limit": {"streamer": {"attempt": 3, "cooldown_until": time.time() - 10}}})
    assert state.is_rate_limited("streamer") is False
    # The expired mark is still readable but not "active".
    assert state.get_rate_limit_cooldown_until("streamer") is not None
