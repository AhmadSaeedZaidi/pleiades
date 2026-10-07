"""Failure, concurrency and resource bounds for the backend-neutral handoff."""

import asyncio
from collections.abc import Sequence
from unittest.mock import AsyncMock

import pytest

from tiered_storage import StagedItem, TransferPolicy, promote


class MemoryCold:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.writes: list[list[str]] = []
        self.corrupt: set[str] = set()
        self.active = 0
        self.peak = 0

    async def write_batch(self, items: Sequence[StagedItem]) -> dict[str, str]:
        self.writes.append([item.key for item in items])
        for item in items:
            self.objects[item.path] = item.payload
        return {item.key: item.path for item in items}

    async def read(self, uri: str) -> bytes | None:
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            await asyncio.sleep(0)
            return b"corrupted" if uri in self.corrupt else self.objects.get(uri)
        finally:
            self.active -= 1


def item(key: str = "a", payload: bytes = b"value", version: str = "1") -> StagedItem:
    return StagedItem(key=key, version=version, payload=payload)


async def test_batch_write_and_finalize_once_with_bounded_verification() -> None:
    cold = MemoryCold()
    items = [item(str(i)) for i in range(50)]
    finalize = AsyncMock(return_value={i.key for i in items})
    result = await promote(items, cold, finalize, TransferPolicy(verify_concurrency=2))
    assert len(cold.writes) == 1
    finalize.assert_awaited_once()
    assert cold.peak == 2
    assert len(result.promoted) == 50
    assert not result.failed and not result.deferred


async def test_corrupt_record_stays_hot_while_other_records_complete() -> None:
    cold = MemoryCold()
    bad, good = item("bad"), item("good", b"other")
    cold.corrupt.add(bad.path)
    finalize = AsyncMock(return_value={"good"})
    result = await promote([bad, good], cold, finalize)
    assert result.promoted == ("good",)
    assert result.failed[0].key == "bad"
    assert result.failed[0].stage == "verify"
    assert [stored.item.key for stored in finalize.await_args.args[0]] == ["good"]


async def test_write_failure_preserves_entire_hot_batch() -> None:
    cold = MemoryCold()
    cold.write_batch = AsyncMock(side_effect=OSError("private endpoint details"))
    finalize = AsyncMock()
    result = await promote([item()], cold, finalize)
    finalize.assert_not_awaited()
    assert result.failed[0].error == "OSError"
    assert "private" not in repr(result)
    cold.write_batch.assert_awaited_once()  # retries belong to the backend/scheduler


@pytest.mark.parametrize("receipts", [{}, {"a": ""}, {"a": "uri", "extra": "uri"}])
async def test_incomplete_receipts_never_finalize(receipts: dict[str, str]) -> None:
    cold = MemoryCold()
    cold.write_batch = AsyncMock(return_value=receipts)
    finalize = AsyncMock()
    result = await promote([item()], cold, finalize)
    assert result.failed[0].stage == "write"
    finalize.assert_not_awaited()


async def test_changed_version_is_deferred() -> None:
    result = await promote([item()], MemoryCold(), AsyncMock(return_value=set()))
    assert result.deferred == ("a",)
    assert not result.failed and not result.promoted


async def test_verified_existing_cold_copy_skips_upload() -> None:
    cold = MemoryCold()
    staged = StagedItem("a", "1", b"value", existing_uri="legacy")
    cold.objects["legacy"] = b"value"
    result = await promote([staged], cold, AsyncMock(return_value={"a"}))
    assert result.promoted == ("a",)
    assert not cold.writes


@pytest.mark.parametrize("previous", [None, b"corrupt"])
async def test_invalid_existing_cold_copy_is_repaired_before_finalization(previous) -> None:
    cold = MemoryCold()
    if previous is not None:
        cold.objects["legacy"] = previous
    staged = StagedItem("a", "1", b"value", existing_uri="legacy")
    result = await promote([staged], cold, AsyncMock(return_value={"a"}))
    assert result.promoted == ("a",)
    assert cold.writes == [["a"]]


async def test_finalize_failure_can_retry_same_immutable_objects() -> None:
    cold = MemoryCold()
    finalize = AsyncMock(side_effect=[OSError(), {"a"}])
    first = await promote([item()], cold, finalize)
    second = await promote([item()], cold, finalize)
    assert first.failed[0].stage == "finalize"
    assert second.promoted == ("a",)
    assert len(cold.objects) == 1


async def test_cancellation_during_write_never_clears_hot_data() -> None:
    cold = MemoryCold()
    cold.write_batch = AsyncMock(side_effect=asyncio.CancelledError)
    finalize = AsyncMock()
    with pytest.raises(asyncio.CancelledError):
        await promote([item()], cold, finalize)
    finalize.assert_not_awaited()


async def test_byte_budget_splits_batches_and_rejects_oversized_objects() -> None:
    cold = MemoryCold()
    finalize = AsyncMock(side_effect=lambda rows: {row.item.key for row in rows})
    result = await promote(
        [item("a", b"123"), item("b", b"456"), item("large", b"1234567")],
        cold,
        finalize,
        TransferPolicy(max_batch_bytes=5),
    )
    assert cold.writes == [["a"], ["b"]]
    assert result.promoted == ("a", "b")
    assert result.failed[0].key == "large"
    assert result.failed[0].stage == "select"


async def test_duplicate_keys_rejected_before_any_side_effect() -> None:
    cold = MemoryCold()
    with pytest.raises(ValueError, match="Duplicate"):
        await promote([item(), item(payload=b"different")], cold, AsyncMock())
    assert not cold.writes


@pytest.mark.parametrize(
    "kwargs", [{"batch_size": 0}, {"max_batch_bytes": 0}, {"verify_concurrency": 0}]
)
def test_policy_rejects_unbounded_or_invalid_settings(kwargs: dict[str, int]) -> None:
    with pytest.raises(ValueError):
        TransferPolicy(**kwargs)
