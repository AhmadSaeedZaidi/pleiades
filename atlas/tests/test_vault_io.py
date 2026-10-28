"""Vault I/O must not share a thread pool with media work.

The default asyncio executor is ``min(32, cpu+4)`` — 6 threads on the 2-core
executor box. Vault writes are synchronous and sleep on HTTP 429 back-off, so
dispatching them to that shared pool meant a few concurrent back-offs could park
every thread and silently stall all yt-dlp downloads and ffmpeg extraction: no
exception, no log line, just a pipeline that stops making progress. These tests
pin the isolation.
"""

from __future__ import annotations

import asyncio
import threading

import pytest
from atlas.vault_io import (
    _shutdown_vault_executor,
    _vault_executor,
    run_vault_io,
)

from atlas import vault_io


@pytest.fixture(autouse=True)
def _reset_executor():
    _shutdown_vault_executor()
    yield
    _shutdown_vault_executor()


def test_vault_work_runs_on_the_isolated_pool() -> None:
    """The executing thread must be a vault-io thread, not a default-pool one."""
    seen: list[str] = []

    async def scenario() -> list[str]:
        await run_vault_io(lambda: seen.append(threading.current_thread().name))
        return seen

    names = asyncio.run(scenario())
    assert names, "vault operation did not run"
    assert all(name.startswith("vault-io") for name in names), names
    assert threading.current_thread().name not in names, "ran inline on the event loop"


def test_pool_is_small_and_bounded() -> None:
    executor = _vault_executor()
    assert executor._max_workers == vault_io._VAULT_WORKERS  # noqa: SLF001
    assert vault_io._VAULT_WORKERS == 2, "a wider pool just parks more threads"


def test_pool_is_lazy_and_singleton() -> None:
    assert _vault_executor() is _vault_executor()


def test_shutdown_is_idempotent_and_recreates() -> None:
    first = _vault_executor()
    _shutdown_vault_executor()
    _shutdown_vault_executor()  # must not raise
    second = _vault_executor()
    assert first is not second
    assert first._shutdown  # noqa: SLF001


async def _wait_for_vault_threads(count: int, timeout: float = 2.0) -> bool:
    """Yield until `count` vault-io threads exist, or the timeout expires.

    Deliberately avoids ``asyncio.sleep``: maia's conftest patches it globally
    with an AsyncMock, so awaiting it is a no-op and never yields to the loop.
    ``asyncio.to_thread`` is a real yield.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        alive = [t for t in threading.enumerate() if t.name.startswith("vault-io")]
        if len(alive) >= count:
            return True
        await asyncio.to_thread(lambda: None)
    return False


def test_saturated_vault_does_not_starve_media_work() -> None:
    """Fill the vault pool with blocking work; the default pool must still work.

    This is the actual regression: before the fix, the vault back-offs ran on the
    default executor, so saturating vault work would have made the media-shaped
    call below block too, and the whole fleet would stall with no error.
    """
    release = threading.Event()

    async def scenario() -> tuple[bool, float]:
        # Saturate every vault thread with work that blocks until released.
        blockers = [asyncio.create_task(run_vault_io(release.wait)) for _ in range(8)]
        assert await _wait_for_vault_threads(vault_io._VAULT_WORKERS), "vault pool never filled"

        # Media-shaped work on the *default* executor must still complete promptly.
        loop = asyncio.get_running_loop()
        started = loop.time()
        assert await loop.run_in_executor(None, lambda: True) is True
        elapsed = loop.time() - started

        release.set()
        await asyncio.gather(*blockers, return_exceptions=True)
        return True, elapsed

    _, elapsed = asyncio.run(scenario())
    assert elapsed < 1.0, f"default executor was starved by vault back-off ({elapsed:.2f}s)"


def test_backend_failure_is_not_retried_by_executor() -> None:
    calls = 0

    def failing() -> None:
        nonlocal calls
        calls += 1
        raise OSError("backend exhausted its own retries")

    with pytest.raises(OSError):
        asyncio.run(run_vault_io(failing))
    assert calls == 1
