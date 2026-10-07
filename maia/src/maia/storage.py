"""Shared vault batch-store + per-video DB-marker plumbing.

The artifact fetchers (streamer / singer / muralist) all do the identical thing
once they have extracted bytes for a batch of videos:

* write every artifact to the vault in **ONE** commit (to stay under HuggingFace's
  128-commits/hour account cap), then
* mark each video safe, or — on a vault/network failure — roll every video back
  (release to PENDING or mark FAILED) so a transient blip never strands good work.

:func:`commit_artifacts` centralises that pattern so each agent's
``store_results`` is a one-liner instead of a copy-pasted try/except/loop.
(Painter uses a bespoke vault API — ``store_visual_evidence_batch`` — and keeps
its own ``store_results``.)
"""

from __future__ import annotations

import io
import logging
from collections.abc import Awaitable, Callable
from typing import Any

logger = logging.getLogger(__name__)


async def commit_artifacts(
    *,
    items: list[tuple[str, bytes]],
    video_ids: list[str],
    mark_safe: Callable[[str], Awaitable[None]],
    on_failure: Callable[[str], Awaitable[None]],
    label: str,
    store: Callable[[Callable[[], Any]], Awaitable[Any]] | None = None,
    vault: Any | None = None,
) -> None:
    """Write ``items`` (``vault_rel_path, bytes``) to the vault in ONE commit, then
    mark every ``video_ids`` entry safe. On failure, roll every id back via
    ``on_failure``.

    ``video_ids`` may contain repeats relative to ``items`` (e.g. many frames map
    to one video id); each id is marked exactly once via de-duplication.

    ``store`` / ``vault`` are injectable so unit tests can patch the vault write;
    when omitted they default to ``maia.utils.vault_op_with_retry`` and the live
    ``atlas.vault.get_vault()``.
    """
    if not items:
        return
    run_logger = logger
    if vault is None:
        from atlas.vault import get_vault as _get_vault

        vault = _get_vault()
    elif callable(vault):
        # Callers may pass the ``get_vault`` factory rather than the live
        # instance. Resolve it so the lambda below can call
        # ``vault.store_batch(...)`` — otherwise ``get_vault.store_batch`` raises
        # AttributeError and *every* batch store fails to persist (regression
        # introduced with ``commit_artifacts`` in P3).
        vault = vault()
    if store is None:
        from maia.utils import vault_op_with_retry as _store

        store = _store
    try:
        await store(lambda: vault.store_batch([(path, io.BytesIO(data)) for path, data in items]))
        for vid in dict.fromkeys(video_ids):
            await mark_safe(vid)
        run_logger.info(f"Batched {len(items)} {label} into ONE vault commit")
    except Exception as e:
        run_logger.exception(f"Batched {label} store failed ({len(items)} items): {e}")
        for vid in dict.fromkeys(video_ids):
            await on_failure(vid)
