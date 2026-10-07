"""Bounded executor shared by every vault operation, separate from media work."""

import asyncio
import atexit
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

_VAULT_EXECUTOR: ThreadPoolExecutor | None = None
_VAULT_WORKERS = 2


def _vault_executor() -> ThreadPoolExecutor:
    global _VAULT_EXECUTOR
    if _VAULT_EXECUTOR is None:
        _VAULT_EXECUTOR = ThreadPoolExecutor(
            max_workers=_VAULT_WORKERS, thread_name_prefix="vault-io"
        )
    return _VAULT_EXECUTOR


def _shutdown_vault_executor(wait: bool = False) -> None:
    global _VAULT_EXECUTOR
    executor, _VAULT_EXECUTOR = _VAULT_EXECUTOR, None
    if executor is not None:
        executor.shutdown(wait=wait, cancel_futures=True)


atexit.register(_shutdown_vault_executor)


async def run_vault_io[T](fn: Callable[[], T]) -> T:
    """Run once. Vault backends already own bounded retry/backoff policies.

    Cancelling an await cannot stop an active blocking SDK call. It can leave
    an orphaned cold object, but callers must not finalize unverified hot rows.
    Python still joins active executor threads at interpreter shutdown.
    """
    return await asyncio.get_running_loop().run_in_executor(_vault_executor(), fn)
