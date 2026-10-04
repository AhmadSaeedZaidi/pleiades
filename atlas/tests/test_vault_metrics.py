"""Hermetic tests for deterministic, manifest-backed cold metrics."""

import hashlib
import io
import json
from datetime import UTC, datetime
from typing import Any

import pytest

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

import atlas.vault as vault_module
from atlas.vault import (
    MetricsFileManifest,
    MetricsManifest,
    VaultStrategy,
    metrics_batch_id,
)

if not vault_module.HAS_PANDAS:
    pytest.skip("metrics tests require the Atlas HF/pandas extras", allow_module_level=True)


class MemoryVault(VaultStrategy):
    """Small object-store double that preserves the backend write contract."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.store_calls: list[list[str]] = []
        self.fail_before_manifest_once = False

    def store_json(self, path: str, data: Any) -> None:
        self.files[path] = json.dumps(data).encode("utf-8")

    def fetch_json(self, path: str) -> dict[Any, Any] | None:
        payload = self.files.get(path)
        return json.loads(payload) if payload is not None else None

    def list_files(self, prefix: str) -> list[str]:
        return sorted(path for path in self.files if path.startswith(prefix))

    def store_visual_evidence(
        self, video_id: str, frames: list[tuple[int, bytes]], ext: str = "webp"
    ) -> None:
        del video_id, frames, ext

    def store_visual_evidence_batch(
        self, entries: list[tuple[str, list[tuple[int, bytes]], str]]
    ) -> None:
        del entries

    def store_batch(self, items: list[tuple[str, Any]]) -> list[str]:
        self.store_calls.append([path for path, _ in items])
        stored: list[str] = []
        for path, data in items:
            if path.startswith("metrics/manifests/") and self.fail_before_manifest_once:
                self.fail_before_manifest_once = False
                raise RuntimeError("simulated failure before manifest")
            if isinstance(data, io.BytesIO):
                payload = data.getvalue()
            else:
                payload = json.dumps(data).encode("utf-8")
            self.files[path] = payload
            stored.append(path)
        return [f"memory://{path}" for path in stored]

    def fetch_binary(self, path: str) -> io.BytesIO | None:
        payload = self.files.get(path.removeprefix("memory://"))
        return io.BytesIO(payload) if payload is not None else None

    def delete_files(self, paths: list[str]) -> int:
        deleted = 0
        for path in paths:
            if path in self.files:
                del self.files[path]
                deleted += 1
        return deleted

    def append_metrics(
        self,
        data: list[dict[Any, Any]],
        date: str | None = None,
        hour: str | None = None,
    ) -> None:
        del data, date, hour


def _rows() -> list[dict[str, Any]]:
    return [
        {
            "video_id": "video-b",
            "views": 20,
            "likes": None,
            "comment_count": 2,
            "timestamp": datetime(2026, 1, 2, 3, 4, tzinfo=UTC),
        },
        {
            "video_id": "video-a",
            "views": 10,
            "likes": 1,
            "comment_count": 0,
            "timestamp": datetime(2026, 1, 1, 23, 59, tzinfo=UTC),
        },
    ]


def test_batch_identity_and_paths_are_order_independent() -> None:
    rows = _rows()
    vault = MemoryVault()

    first = vault.store_metrics_batch(rows)
    second = vault.store_metrics_batch(list(reversed(rows)))

    assert first == second
    assert first.batch_id == metrics_batch_id(rows)
    assert first.path == f"metrics/manifests/{first.batch_id}.json"
    assert [file.path for file in first.files] == [
        f"metrics/date=2026-01-01/hour=23/{first.batch_id}.parquet",
        f"metrics/date=2026-01-02/hour=03/{first.batch_id}.parquet",
    ]
    assert len(vault.store_calls) == 1


def test_completed_manifest_retry_does_not_store_again() -> None:
    vault = MemoryVault()
    rows = _rows()

    manifest = vault.store_metrics_batch(rows)
    retried = vault.store_metrics_batch(rows)

    assert retried == manifest
    assert len(vault.store_calls) == 1


def test_completed_retry_skips_parquet_serialization(monkeypatch) -> None:
    vault = MemoryVault()
    rows = _rows()
    manifest = vault.store_metrics_batch(rows)

    def forbidden(*args, **kwargs):
        pytest.fail("A completed retry must not serialize Parquet again")

    monkeypatch.setattr(vault_module.pd.DataFrame, "to_parquet", forbidden)
    assert vault.store_metrics_batch(rows) == manifest


async def test_document_handoff_writes_one_commit_and_preserves_legacy_body() -> None:
    from unittest.mock import AsyncMock

    from atlas.storage import VaultColdStore
    from atlas.vault import transcript_index_path, transcript_path

    from tiered_storage import StagedItem, promote

    vault = MemoryVault()
    legacy = transcript_path("V1")
    vault.store_json(legacy, [{"text": "old"}])
    original = vault.files[legacy]
    item = StagedItem(
        "V1", "123", b'[{"text":"new"}]', namespace="transcripts/V1/V1", suffix=".json"
    )
    cold = VaultColdStore(vault, heads={"V1": transcript_index_path("V1")})
    result = await promote([item], cold, AsyncMock(return_value={"V1"}))
    assert result.promoted == ("V1",)
    assert len(vault.store_calls) == 1
    assert vault.files[legacy] == original
    assert vault.fetch_transcript("V1") == [{"text": "new"}]
    vault.files[item.path] = b"corrupt"
    with pytest.raises(ValueError, match="digest"):
        vault.fetch_transcript("V1")


async def test_legacy_transcript_reconciliation_verifies_without_upload() -> None:
    from unittest.mock import AsyncMock

    from atlas.storage import VaultColdStore

    from tiered_storage import StagedItem, promote

    vault = MemoryVault()
    vault.store_json("transcripts/V1.json", [{"text": "héllo", "start": 0}])
    uri = "memory://transcripts/V1.json"
    item = StagedItem("V1", "123", '[{"start":0,"text":"héllo"}]'.encode(), existing_uri=uri)
    cold = VaultColdStore(vault, legacy_json_uris={uri})
    finalize = AsyncMock(return_value={"V1"})
    assert (await promote([item], cold, finalize)).promoted == ("V1",)
    assert not vault.store_calls
    assert finalize.await_args.args[0][0].uri == uri


def test_nullable_large_integers_round_trip_without_precision_loss() -> None:
    rows = _rows()
    rows[0]["timestamp"] = rows[1]["timestamp"]
    rows[0]["likes"] = 2**60 + 1
    rows[1]["likes"] = None
    vault = MemoryVault()
    manifest = vault.store_metrics_batch(rows)
    read = vault.read_metrics_batch(manifest)
    assert next(row["likes"] for row in read if row["video_id"] == "video-b") == 2**60 + 1


@pytest.mark.parametrize("counter", [7, 2**60 + 1])
def test_legacy_float_counters_require_original_integer_identity(counter: int) -> None:
    rows = _rows()
    rows[0]["timestamp"] = rows[1]["timestamp"]
    rows[0]["likes"] = counter
    rows[1]["likes"] = None
    vault = MemoryVault()
    batch_id = metrics_batch_id(rows)
    buffer = io.BytesIO()
    # Match the previous writer's nullable integer -> double inference.
    legacy_rows = vault_module._canonical_metric_rows(rows)
    vault_module.pd.DataFrame(legacy_rows).to_parquet(buffer, engine="pyarrow", index=False)
    payload = buffer.getvalue()
    file = MetricsFileManifest(
        path=f"metrics/date=2026-01-01/hour=23/{batch_id}.parquet",
        date="2026-01-01",
        hour="23",
        row_count=2,
        sha256=hashlib.sha256(payload).hexdigest(),
    )
    manifest = MetricsManifest(version=1, batch_id=batch_id, row_count=2, files=(file,))
    vault.files[file.path] = payload
    vault.store_json(manifest.path, manifest.to_dict())
    if counter == 7:
        assert vault.store_metrics_batch(rows) == manifest
        assert not vault.store_calls
        assert metrics_batch_id(vault.read_metrics_batch(manifest)) == batch_id
    else:
        with pytest.raises(ValueError, match="identity mismatch"):
            vault.store_metrics_batch(rows)
        assert not vault.store_calls


@pytest.mark.parametrize("legacy", [False, True])
def test_audio_reader_supports_sharded_and_legacy_files(legacy: bool) -> None:
    from atlas.vault import audio_path

    vault = MemoryVault()
    path = "audio/ABC.opus" if legacy else audio_path("ABC")
    if not legacy:
        vault.files["audio/ABC.opus"] = b"STALE"
    vault.files[path] = b"OPUS"
    audio = vault.fetch_audio("ABC")
    assert audio is not None
    with audio:
        assert audio.read() == b"OPUS"
    assert audio_path("ABC", 2) == "media/audio/AB/ABC/002.opus"


def test_partial_write_before_manifest_retries_same_paths() -> None:
    vault = MemoryVault()
    vault.fail_before_manifest_once = True
    rows = _rows()

    with pytest.raises(RuntimeError, match="before manifest"):
        vault.store_metrics_batch(rows)

    manifest = vault.store_metrics_batch(rows)

    assert len(vault.store_calls) == 2
    assert vault.store_calls[0] == vault.store_calls[1]
    assert manifest.path in vault.files
    assert all(file.path in vault.files for file in manifest.files)


@pytest.mark.parametrize("failure", ["missing", "corrupt"])
def test_missing_or_corrupt_artifact_fails_closed(failure: str) -> None:
    vault = MemoryVault()
    rows = _rows()
    manifest = vault.store_metrics_batch(rows)
    target = manifest.files[0].path

    if failure == "missing":
        del vault.files[target]
        expected = FileNotFoundError
    else:
        vault.files[target] = b"not parquet"
        expected = ValueError

    with pytest.raises(expected):
        vault.store_metrics_batch(rows)


def test_manifest_and_path_validation_fails_closed() -> None:
    file_manifest = MetricsFileManifest(
        path="metrics/date=2026-01-01/hour=01/" + "b" * 64 + ".parquet",
        date="2026-01-01",
        hour="01",
        row_count=1,
        sha256="b" * 64,
    )
    with pytest.raises(ValueError, match="path"):
        MetricsManifest(
            version=1,
            batch_id="a" * 64,
            row_count=1,
            files=(file_manifest,),
        )

    raw = {
        "version": 1,
        "batch_id": "a" * 64,
        "row_count": 1,
        "files": [
            {
                "path": "metrics/date=2026-01-01/hour=24/" + "a" * 64 + ".parquet",
                "date": "2026-01-01",
                "hour": "24",
                "row_count": 1,
                "sha256": "b" * 64,
            }
        ],
    }
    with pytest.raises(ValueError, match="partition"):
        MetricsManifest.from_dict(raw)


def test_reader_filters_and_deduplicates_by_video_timestamp() -> None:
    vault = MemoryVault()
    rows = _rows()
    vault.store_metrics_batch(rows)
    extra = {
        "video_id": "video-c",
        "views": 30,
        "likes": 3,
        "comment_count": 3,
        "timestamp": "2026-01-03T05:00:00+00:00",
    }
    vault.store_metrics_batch([rows[0], extra])

    result = vault.read_metrics(start_date="2026-01-02", video_ids={"video-b", "video-c"})

    assert {(row["video_id"], row["timestamp"]) for row in result} == {
        ("video-b", "2026-01-02T03:04:00+00:00"),
        ("video-c", "2026-01-03T05:00:00+00:00"),
    }


def test_reader_rejects_conflicting_duplicate_rows() -> None:
    vault = MemoryVault()
    rows = _rows()
    vault.store_metrics_batch(rows)
    conflicting = dict(rows[0])
    conflicting["views"] = 999
    vault.store_metrics_batch([conflicting])

    with pytest.raises(ValueError, match="Conflicting metrics rows"):
        vault.read_metrics()


def test_none_and_scalar_normalization_survives_parquet_round_trip() -> None:
    numpy = pytest.importorskip("numpy")
    rows = [
        {
            "video_id": "scalar",
            "views": numpy.int64(1),
            "likes": None,
            "comment_count": numpy.int64(0),
            "ratio": numpy.float64(1.5),
            "timestamp": "2026-01-04T06:00:00Z",
        }
    ]
    vault = MemoryVault()

    manifest = vault.store_metrics_batch(rows)
    result = vault.read_metrics()

    assert manifest.batch_id == metrics_batch_id(result)
    assert result == [
        {
            "video_id": "scalar",
            "views": 1,
            "likes": None,
            "comment_count": 0,
            "ratio": 1.5,
            "timestamp": "2026-01-04T06:00:00Z",
        }
    ]
