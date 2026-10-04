"""Write, verify, then conditionally release hot payloads in bounded batches."""

import asyncio
import hashlib
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Protocol


@dataclass(frozen=True, slots=True)
class StagedItem:
    key: str
    version: str
    payload: bytes = field(repr=False)
    namespace: str = "objects"
    suffix: str = ".bin"
    existing_uri: str | None = None
    sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if not self.key or not self.version or not isinstance(self.payload, bytes):
            raise ValueError("A staged item requires a key, version and bytes payload")
        parts = PurePosixPath(self.namespace).parts
        if (
            not parts
            or self.namespace.startswith("/")
            or any(part in (".", "..") for part in parts)
            or "\\" in self.namespace
            or not self.suffix.startswith(".")
            or any(char in self.suffix for char in ("/", "\\"))
        ):
            raise ValueError("Invalid object namespace or suffix")
        object.__setattr__(self, "sha256", hashlib.sha256(self.payload).hexdigest())

    @property
    def path(self) -> str:
        """Immutable path: a newer payload cannot overwrite an older receipt."""
        return f"{self.namespace}/{self.sha256}{self.suffix}"


@dataclass(frozen=True, slots=True)
class StoredItem:
    item: StagedItem
    uri: str


@dataclass(frozen=True, slots=True)
class HotObject:
    payload: bytes | None = field(repr=False)
    uri: str | None
    sha256: str


class HotStore(Protocol):
    async def pending(self, limit: int) -> Sequence[StagedItem]:
        """Select at most limit eligible items. Do not hold locks during upload."""
        ...

    async def finalize(self, items: Sequence[StoredItem]) -> set[str]:
        """Atomically record URIs/clear payloads only for matching key+version.

        Return keys actually finalized. Changed versions stay hot. This must
        be a transaction, not an unconditional delete after an earlier check.
        """
        ...

    async def get(self, key: str) -> HotObject | None: ...


class ColdStore(Protocol):
    async def write_batch(self, items: Sequence[StagedItem]) -> Mapping[str, str]:
        """Idempotently write immutable paths, returning one durable URI per key."""
        ...

    async def read(self, uri: str) -> bytes | None: ...


@dataclass(frozen=True, slots=True)
class TransferPolicy:
    batch_size: int = 50
    max_batch_bytes: int = 16 * 1024 * 1024
    verify_concurrency: int = 4

    def __post_init__(self) -> None:
        if min(self.batch_size, self.max_batch_bytes, self.verify_concurrency) < 1:
            raise ValueError(
                "Batch size, byte budget and verification concurrency must be positive"
            )


@dataclass(frozen=True, slots=True)
class TransferFailure:
    key: str
    stage: str
    error: str  # exception class only; never backend credentials or payloads


@dataclass(frozen=True, slots=True)
class TransferResult:
    promoted: tuple[str, ...] = ()
    deferred: tuple[str, ...] = ()
    failed: tuple[TransferFailure, ...] = ()

    @property
    def selected(self) -> int:
        return len(self.promoted) + len(self.deferred) + len(self.failed)


async def promote(
    items: Sequence[StagedItem],
    cold: ColdStore,
    finalize: Callable[[Sequence[StoredItem]], Awaitable[set[str]]],
    policy: TransferPolicy | None = None,
) -> TransferResult:
    """Transfer selected items. Failures leave hot state available for retry.

    There is no distributed transaction. Immutable cold objects may remain
    after failure/cancellation, and retries reuse them. A failed verification
    affects that record only. Backend retry policy belongs to the backend.
    """
    policy = policy or TransferPolicy()
    if len({item.key for item in items}) != len(items):
        raise ValueError("Duplicate staged keys")
    batches: list[list[StagedItem]] = []
    failures: list[TransferFailure] = []
    chunk: list[StagedItem] = []
    size = 0
    for item in items:
        if len(item.payload) > policy.max_batch_bytes:
            failures.append(TransferFailure(item.key, "select", "ObjectTooLarge"))
            continue
        if chunk and (
            len(chunk) >= policy.batch_size or size + len(item.payload) > policy.max_batch_bytes
        ):
            batches.append(chunk)
            chunk, size = [], 0
        chunk.append(item)
        size += len(item.payload)
    if chunk:
        batches.append(chunk)

    promoted: list[str] = []
    deferred: list[str] = []
    semaphore = asyncio.Semaphore(policy.verify_concurrency)

    async def verify(item: StagedItem, uri: str, *, reuse: bool = False) -> StoredItem | None:
        try:
            async with semaphore:
                payload = await cold.read(uri)
                if payload is None or hashlib.sha256(payload).hexdigest() != item.sha256:
                    raise ValueError("Cold object digest mismatch or object missing")
            return StoredItem(item, uri)
        except Exception as exc:
            if not reuse:
                failures.append(TransferFailure(item.key, "verify", type(exc).__name__))
            return None

    for batch in batches:
        checked_existing = await asyncio.gather(
            *(verify(item, item.existing_uri, reuse=True) for item in batch if item.existing_uri)
        )
        reused = {stored.item.key: stored for stored in checked_existing if stored is not None}
        uploads = [item for item in batch if item.key not in reused]
        checked: list[StoredItem | None] = []
        try:
            receipts = await cold.write_batch(uploads) if uploads else {}
            if set(receipts) != {item.key for item in uploads} or any(
                not isinstance(uri, str) or not uri.strip() for uri in receipts.values()
            ):
                raise ValueError("Cold backend returned incomplete receipts")
        except Exception as exc:
            failures.extend(
                TransferFailure(item.key, "write", type(exc).__name__) for item in uploads
            )
        else:
            checked = await asyncio.gather(*(verify(item, receipts[item.key]) for item in uploads))
        successful = reused | {stored.item.key: stored for stored in checked if stored is not None}
        verified = [successful[item.key] for item in batch if item.key in successful]
        if not verified:
            continue
        try:
            completed = await finalize(verified)
            if not completed <= {stored.item.key for stored in verified}:
                raise ValueError("Hot backend returned unknown finalized keys")
        except Exception as exc:
            failures.extend(
                TransferFailure(stored.item.key, "finalize", type(exc).__name__)
                for stored in verified
            )
            continue
        for stored in verified:
            (promoted if stored.item.key in completed else deferred).append(stored.item.key)
    return TransferResult(tuple(promoted), tuple(deferred), tuple(failures))


class TieredStorage:
    def __init__(
        self, hot: HotStore, cold: ColdStore, policy: TransferPolicy | None = None
    ) -> None:
        self.hot, self.cold, self.policy = hot, cold, policy or TransferPolicy()

    async def flush(self) -> TransferResult:
        items = await self.hot.pending(self.policy.batch_size)
        if len(items) > self.policy.batch_size:
            raise ValueError("Hot backend exceeded the selection limit")
        return await promote(items, self.cold, self.hot.finalize, self.policy)

    async def read(self, key: str) -> bytes | None:
        record = await self.hot.get(key)
        if record is None:
            return None
        payload = record.payload
        if payload is None:
            if not record.uri:
                raise ValueError("Hot record has neither a payload nor a cold URI")
            payload = await self.cold.read(record.uri)
            if payload is None:
                raise FileNotFoundError("Referenced cold object is missing")
        if hashlib.sha256(payload).hexdigest() != record.sha256:
            raise ValueError("Stored object digest mismatch")
        return payload
