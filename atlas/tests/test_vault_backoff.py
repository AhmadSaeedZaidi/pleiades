"""Back-off budget and executor-isolation contracts for Vault I/O.

These are the regression guards for a failure mode with no error message: vault
retries used to sleep inside a thread of the *shared* default executor, so a few
concurrent back-offs silently stalled every yt-dlp download and ffmpeg call in
the fleet.
"""

from __future__ import annotations

import inspect
import io

import pytest
from atlas.vault import HuggingFaceVault, VaultStrategy, _is_permanent, _is_rate_limited

from atlas import vault as vault_module


class FlakyApi:
    """HfApi double that fails a fixed number of times before succeeding."""

    def __init__(self, failures: int, error: Exception) -> None:
        self.failures = failures
        self.error = error
        self.calls = 0

    def create_commit(self, **kwargs):  # noqa: ANN003
        self.calls += 1
        if self.calls <= self.failures:
            raise self.error
        return object()


def _vault(api: FlakyApi) -> HuggingFaceVault:
    v = HuggingFaceVault.__new__(HuggingFaceVault)
    v.api = api
    v.repo_id = "example/vault"
    v.token = None
    return v


def test_rate_limit_detection_covers_google_api_core_shape() -> None:
    class GoogleApiError(Exception):
        code = 429

    class ResponseStatusError(Exception):
        class response:  # noqa: N801
            status_code = 429

    assert _is_rate_limited(GoogleApiError("slow down")) is True
    assert _is_rate_limited(ResponseStatusError("slow down")) is True
    assert _is_rate_limited(RuntimeError("connection reset")) is False


@pytest.mark.parametrize("status", [401, 403, 404, 410])
def test_permanent_errors_are_classified(status: int) -> None:
    class HttpStatusError(Exception):
        class response:  # noqa: N801
            status_code = status

    assert _is_permanent(HttpStatusError("nope")) is True


def test_transient_errors_are_not_permanent() -> None:
    class HttpStatusError(Exception):
        class response:  # noqa: N801
            status_code = 503

    assert _is_permanent(HttpStatusError("unavailable")) is False


def test_permanent_failure_is_not_retried() -> None:
    """A revoked token must fail immediately, not after the whole budget."""
    api = FlakyApi(failures=99, error=RuntimeError("401 unauthorized"))
    v = _vault(api)
    with patch_sleep() as slept:
        with pytest.raises(RuntimeError, match="401"):
            v.store_batch([("a.json", {"x": 1})])
    assert api.calls == 1, "permanent error must not be retried"
    assert slept == []


def test_transient_backoff_budget_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    """The total back-off must stay small: this runs in a worker thread."""
    slept: list[float] = []
    monkeypatch.setattr(vault_module.time, "sleep", lambda s: slept.append(s))
    monkeypatch.setattr(vault_module.random, "random", lambda: 1.0)  # worst-case jitter

    api = FlakyApi(failures=99, error=RuntimeError("503 service unavailable"))
    v = _vault(api)
    with pytest.raises(RuntimeError):
        v.store_batch([("a.json", {"x": 1})])

    total = sum(slept)
    assert api.calls == 5, "attempt budget should be 5"
    # Worst case with jitter forced to 1.0: 10 + 20 + 40 + 80 = 150s.
    assert total <= 150, f"back-off budget too large: {total}s"
    assert total < 300, "regression: multi-minute/hours-long back-off returned"


def test_backoff_includes_jitter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Identical delays across agents would produce synchronised retry storms."""
    values = iter([0.0, 0.0])
    monkeypatch.setattr(vault_module.random, "random", lambda: next(values))
    slept: list[float] = []
    monkeypatch.setattr(vault_module.time, "sleep", lambda s: slept.append(s))

    v = _vault(FlakyApi(failures=1, error=RuntimeError("503")))
    assert v.store_batch([("a.json", {"x": 1})])
    assert len(slept) == 1
    assert slept[0] == pytest.approx(5.0)  # 10 * (0.5 + 0.0 * 0.5)


def test_empty_batch_never_calls_the_api(monkeypatch: pytest.MonkeyPatch) -> None:
    v = _vault(FlakyApi(failures=99, error=RuntimeError("boom")))
    assert v.store_batch([]) == []


class patch_sleep:  # noqa: N801
    def __enter__(self) -> list[float]:
        self._slept: list[float] = []
        self._orig = vault_module.time.sleep
        vault_module.time.sleep = lambda s: self._slept.append(s)
        return self._slept

    def __exit__(self, *exc: object) -> None:
        vault_module.time.sleep = self._orig


def test_store_batch_writes_bytesio_payloads() -> None:
    api = FlakyApi(failures=0, error=RuntimeError("unused"))
    v = _vault(api)
    out = v.store_batch([("frames/a.webp", io.BytesIO(b"binary-bytes"))])
    assert out == ["hf://datasets/example/vault/frames/a.webp"]


def test_vault_strategy_is_abstract() -> None:
    with pytest.raises(TypeError):
        VaultStrategy()  # type: ignore[abstract]


def test_store_batch_retry_budget_stays_small() -> None:
    """The attempt budget is a thread-occupancy budget, not just a retry count.

    store_batch runs in a worker thread and sleeps between attempts, so a large
    attempt count multiplied by a large base delay is what caused the ~35 minute
    (and, with the caller's own retry wrapper, ~1.8 hour) parked thread.
    """
    for cls in (HuggingFaceVault, vault_module.GCSVault):
        params = inspect.signature(cls.store_batch).parameters
        assert params["max_attempts"].default <= 5, cls
        assert params["base_delay"].default <= 10.0, cls
