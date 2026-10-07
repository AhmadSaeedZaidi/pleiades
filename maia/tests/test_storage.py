"""Regression tests for :func:`maia.storage.commit_artifacts`.

The production store failure (``AttributeError`` — ``get_vault.store_batch``)
was invisible to the unit suite because the agents' tests patch
``vault_op_with_retry`` with an :class:`~unittest.mock.AsyncMock`, which records
the call without *executing* the lambda that calls ``vault.store_batch``. These
tests drive a store callable that actually runs the lambda, so a callable
``vault`` (the ``get_vault`` factory) is exercised the way production does.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from maia.storage import commit_artifacts


async def _run_lambda(fn):
    """A stand-in for ``vault_op_with_retry`` that executes the lambda."""
    return fn()


class _Vault:
    """Non-callable vault stub (a plain instance, unlike ``MagicMock``)."""

    def __init__(self, store_batch):
        self.store_batch = store_batch


async def test_commit_artifacts_runs_store_with_vault_instance():
    """Passing the ``get_vault`` factory (not an instance) must resolve to the
    live instance and call ``store_batch`` on it — the exact shape that raised
    ``AttributeError`` in production (every batch store failed to persist)."""
    import atlas.vault as av

    fake_vault = _Vault(MagicMock(return_value=["raw/x"]))
    original = av.get_vault
    av.get_vault = lambda: fake_vault
    try:
        mark_safe = AsyncMock()
        on_failure = AsyncMock()
        await commit_artifacts(
            items=[("raw/x", b"abc")],
            video_ids=["x"],
            mark_safe=mark_safe,
            on_failure=on_failure,
            label="test raw",
            store=_run_lambda,
            vault=av.get_vault,  # the bug: factory, not instance
        )
    finally:
        av.get_vault = original

    fake_vault.store_batch.assert_called_once()
    mark_safe.assert_awaited_once_with("x")
    on_failure.assert_not_awaited()


async def test_commit_artifacts_rolls_back_on_store_failure():
    """A store exception must roll every video back via ``on_failure``."""
    fake_vault = _Vault(MagicMock(side_effect=RuntimeError("boom")))

    mark_safe = AsyncMock()
    on_failure = AsyncMock()
    await commit_artifacts(
        items=[("raw/x", b"abc")],
        video_ids=["x", "x"],  # duplicate id exercised once
        mark_safe=mark_safe,
        on_failure=on_failure,
        label="test raw",
        store=_run_lambda,
        vault=fake_vault,
    )

    mark_safe.assert_not_awaited()
    on_failure.assert_awaited_once_with("x")
