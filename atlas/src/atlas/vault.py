import abc
import contextlib
import hashlib
import io
import json
import logging
import math
import random
import shutil
import tempfile
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from atlas.config import settings

HAS_GCS = False
try:
    from google.cloud import storage  # type: ignore
    from google.cloud.storage import Client as GCSClient  # type: ignore  # noqa: F401

    HAS_GCS = True
except ImportError:
    pass

HAS_HF = False
HAS_PANDAS = False
try:
    import pandas as pd
    from huggingface_hub import (
        CommitOperationAdd,
        CommitOperationDelete,
        HfApi,
        hf_hub_download,
    )
    from huggingface_hub.errors import EntryNotFoundError, LocalEntryNotFoundError

    HAS_HF = True
    HAS_PANDAS = True
except ImportError:
    pass

if TYPE_CHECKING:
    with contextlib.suppress(ImportError):
        from google.cloud import storage  # noqa: F401

    try:
        import pandas as pd
        from huggingface_hub import (
            CommitOperationAdd,
            CommitOperationDelete,
            HfApi,
            hf_hub_download,
        )
    except ImportError:
        pass

logger = logging.getLogger("atlas.vault")


@dataclass(frozen=True, slots=True)
class MetricsFileManifest:
    """Description of one deterministic Parquet object in a metrics batch."""

    path: str
    date: str
    hour: str
    row_count: int
    sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "date": self.date,
            "hour": self.hour,
            "row_count": self.row_count,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "MetricsFileManifest":
        try:
            path = str(raw["path"])
            date = str(raw["date"])
            hour = str(raw["hour"])
            row_count = int(raw["row_count"])
            sha256 = str(raw["sha256"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("Invalid metrics file manifest") from exc
        try:
            datetime.strptime(date, "%Y-%m-%d")
            parsed_hour = int(hour)
        except ValueError as exc:
            raise ValueError("Invalid metrics file manifest partition") from exc
        if not 0 <= parsed_hour <= 23 or hour != f"{parsed_hour:02d}":
            raise ValueError("Invalid metrics file manifest partition")
        if (
            not path
            or row_count < 1
            or len(sha256) != 64
            or any(character not in "0123456789abcdef" for character in sha256)
        ):
            raise ValueError("Invalid metrics file manifest values")
        return cls(path=path, date=date, hour=hour, row_count=row_count, sha256=sha256)


@dataclass(frozen=True, slots=True)
class MetricsManifest:
    """Content-addressed completion record for one cold metrics batch."""

    version: int
    batch_id: str
    row_count: int
    files: tuple[MetricsFileManifest, ...]

    def __post_init__(self) -> None:
        if (
            self.version != 1
            or len(self.batch_id) != 64
            or any(character not in "0123456789abcdef" for character in self.batch_id)
            or self.row_count < 1
        ):
            raise ValueError("Invalid metrics manifest identity")
        if not self.files or len({file.path for file in self.files}) != len(self.files):
            raise ValueError("Invalid metrics manifest files")
        if sum(file.row_count for file in self.files) != self.row_count:
            raise ValueError("Metrics manifest row count does not match files")
        for file in self.files:
            expected_path = f"metrics/date={file.date}/hour={file.hour}/{self.batch_id}.parquet"
            if file.path != expected_path:
                raise ValueError("Metrics manifest file path does not match its identity")

    @property
    def path(self) -> str:
        return f"metrics/manifests/{self.batch_id}.json"

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "batch_id": self.batch_id,
            "row_count": self.row_count,
            "files": [file.to_dict() for file in self.files],
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "MetricsManifest":
        try:
            version = int(raw["version"])
            batch_id = str(raw["batch_id"])
            row_count = int(raw["row_count"])
            raw_files = raw["files"]
            if not isinstance(raw_files, list):
                raise TypeError("files must be a list")
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("Invalid metrics manifest") from exc
        # Per-file validation runs outside the envelope above so its specific
        # message (bad path, bad partition, bad digest) survives to the caller.
        files = tuple(
            MetricsFileManifest.from_dict(file) for file in raw_files if isinstance(file, Mapping)
        )
        if len(files) != len(raw_files):
            raise ValueError("Invalid metrics manifest file entry")
        return cls(version=version, batch_id=batch_id, row_count=row_count, files=files)


def _canonical_metric_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    # Pandas commonly returns NumPy scalars after a Parquet round trip. Convert
    # those to plain Python before hashing or comparing rows.
    item = getattr(value, "item", None)
    if callable(item):
        with contextlib.suppress(TypeError, ValueError):
            converted = item()
            if converted is not value:
                return _canonical_metric_value(converted)
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(UTC)
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _canonical_metric_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_metric_value(item) for item in value]
    return value


def _canonical_metric_rows(data: Sequence[Mapping[Any, Any]]) -> list[dict[str, Any]]:
    rows = [
        {str(key): _canonical_metric_value(value) for key, value in row.items()} for row in data
    ]
    # Older Parquet writers inferred nullable counters as doubles. Recover exact
    # integer counters before checking their original logical batch identity;
    # rounded or otherwise changed values still fail that identity check.
    for row in rows:
        for key in ("views", "likes", "comment_count"):
            value = row.get(key)
            if isinstance(value, float) and value.is_integer():
                row[key] = int(value)
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("timestamp", "")),
            str(row.get("video_id", "")),
            json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
        ),
    )


def metrics_batch_id(data: Sequence[Mapping[Any, Any]]) -> str:
    """Return the stable content identity for a metrics row set."""
    rows = _canonical_metric_rows(data)
    return _metric_rows_id(rows)


def _metric_rows_id(rows: Sequence[Mapping[str, Any]]) -> str:
    # Internal callers already canonicalized/sorted the rows.
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def metrics_partition(timestamp: Any) -> tuple[str, str]:
    """Return the date/hour partition for a metric timestamp."""
    canonical = _canonical_metric_value(timestamp)
    timestamp_text = str(canonical)
    try:
        parsed = datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid metrics timestamp: {timestamp_text!r}") from exc
    return parsed.strftime("%Y-%m-%d"), parsed.strftime("%H")


def _is_rate_limited(exc: Exception) -> bool:
    """Heuristic detection of HTTP 429 (rate-limit) from a vault SDK error."""
    resp = getattr(exc, "response", None)
    status = getattr(resp, "status_code", None)
    if status == 429:
        return True
    # google.api_core raises TooManyRequests with .code, not .response.status_code.
    if getattr(exc, "code", None) == 429:
        return True
    return "429" in str(exc)


def _is_permanent(exc: Exception) -> bool:
    """True when retrying cannot possibly help (auth, not-found, bad request).

    Without this, a revoked HF token or a 404 path is retried on the same
    schedule as a 429 — burning the full back-off budget to arrive at the same
    failure.
    """
    resp = getattr(exc, "response", None)
    status = getattr(resp, "status_code", None) or getattr(exc, "code", None)
    if status is None:
        text = str(exc).lower()
        return any(
            marker in text
            for marker in ("401", "403", "404", "unauthorized", "forbidden", "not found")
        )
    return int(status) in {401, 403, 404, 410}


def _backoff_sleep(delay: float, attempt: int, max_attempts: int) -> None:
    """Sleep with jitter so concurrent agents do not retry in lockstep."""
    capped = min(delay, 120.0)
    time.sleep(capped * (0.5 + random.random() * 0.5))
    logger.info(
        "Vault backoff complete after attempt %d/%d (slept up to %.0fs)",
        attempt,
        max_attempts,
        capped,
    )


class VaultStrategy(abc.ABC):
    @abc.abstractmethod
    def store_json(self, path: str, data: Any) -> None:
        pass

    @abc.abstractmethod
    def fetch_json(self, path: str) -> dict[Any, Any] | None:
        pass

    @abc.abstractmethod
    def list_files(self, prefix: str) -> list[str]:
        pass

    @abc.abstractmethod
    def store_visual_evidence(
        self, video_id: str, frames: list[tuple[int, bytes]], ext: str = "webp"
    ) -> None:
        """Store keyframes for one video in a single commit."""

    @abc.abstractmethod
    def store_visual_evidence_batch(
        self, entries: list[tuple[str, list[tuple[int, bytes]], str]]
    ) -> None:
        """Store keyframes for many videos in a single HF commit (stays under
        the 128-commits/hour cap)."""
        pass

    @abc.abstractmethod
    def store_batch(self, items: list[tuple[str, Any]]) -> list[str]:
        """Write many files in a single commit (HF) or batched call (GCS)."""
        pass

    @abc.abstractmethod
    def fetch_binary(self, path: str) -> io.BytesIO | None:
        pass

    def fetch_audio(self, video_id: str) -> io.BytesIO | None:
        """Prefer sharded audio, retaining reads of existing flat artifacts."""
        for path in (audio_path(video_id), f"audio/{video_id}.opus"):
            buffer = self.fetch_binary(path)
            if buffer is not None:
                return buffer
        return None

    def fetch_binary_to_path(self, path: str, destination: str | Path) -> bool:
        """Materialize a binary artifact without retaining a second copy.

        Backends with a path-oriented download primitive should override this
        method. The fallback keeps compatibility with older strategies while
        copying the returned buffer to disk in bounded chunks.
        """
        buffer = self.fetch_binary(path)
        if buffer is None:
            return False
        try:
            with Path(destination).open("wb") as output:
                shutil.copyfileobj(buffer, output, length=1024 * 1024)
            return True
        except Exception as e:
            logger.warning(f"Failed to materialize binary {path} to {destination}: {e}")
            return False
        finally:
            buffer.close()

    @abc.abstractmethod
    def delete_files(self, paths: list[str]) -> int:
        """Permanently delete the given repo-relative ``paths``. Returns count deleted."""
        pass

    def store_metadata(self, video_id: str, data: dict[Any, Any], date: str | None = None) -> None:
        if date is None:
            date = datetime.now(UTC).strftime("%Y-%m-%d")
        path = f"metadata/{date}/{video_id}.json"
        self.store_json(path, data)

    def fetch_metadata(self, video_id: str, date: str) -> dict[Any, Any] | None:
        for path in (f"metadata/{date}/{video_id}/index.json", f"metadata/{date}/{video_id}.json"):
            value = self._fetch_document(path)
            if value is not None:
                if not isinstance(value, dict):
                    raise ValueError("Cold metadata must be a JSON object")
                return value
        return None

    def fetch_transcript(self, video_id: str) -> Any | None:
        # New transcripts are sharded (transcripts/{prefix}/{id}.json) to stay
        # under HF's 10k-files-per-directory limit; legacy flat paths still exist.
        for path in (
            transcript_index_path(video_id),
            transcript_path(video_id),
            f"transcripts/{video_id}.json",
        ):
            result = self._fetch_document(path)
            if result is not None:
                return result
        return None

    def _fetch_document(self, path: str) -> Any | None:
        """Read legacy JSON bodies or resolve a verified immutable-object head."""
        value = self.fetch_json(path)
        if not isinstance(value, Mapping) or "_tiered_storage" not in value:
            return value
        target, digest = value.get("path"), value.get("sha256")
        namespace = (
            path.removesuffix("/index.json")
            if path.endswith("/index.json")
            else path.removesuffix(".json")
        )
        if (
            value["_tiered_storage"] != 1
            or not isinstance(target, str)
            or not isinstance(digest, str)
            or len(digest) != 64
            or target != f"{namespace}/{digest}.json"
            or any(char not in "0123456789abcdef" for char in digest)
        ):
            raise ValueError("Invalid tiered storage document pointer")
        buffer = self.fetch_binary(target)
        if buffer is None:
            raise FileNotFoundError("Referenced cold document is missing")
        try:
            payload = buffer.read()
        finally:
            buffer.close()
        if hashlib.sha256(payload).hexdigest() != digest:
            raise ValueError("Cold document digest mismatch")
        return json.loads(payload)

    @abc.abstractmethod
    def append_metrics(
        self,
        data: list[dict[Any, Any]],
        date: str | None = None,
        hour: str | None = None,
    ) -> None:
        pass

    def store_metrics_batch(self, data: list[dict[Any, Any]]) -> MetricsManifest:
        """Store one deterministic, manifest-backed cold metrics batch.

        Parquet objects are written before the manifest.  HF commits the whole
        list atomically; GCS uploads the list in order, so the manifest is the
        completion marker and a retry safely rewrites the same object paths.
        """
        if not data:
            raise ValueError("Cannot store an empty metrics batch")
        if not HAS_PANDAS:
            raise ImportError("Pandas and pyarrow required for metrics batches")

        rows = _canonical_metric_rows(data)
        batch_id = _metric_rows_id(rows)
        # A completed retry must not reserialize Parquet. Its physical encoding
        # may differ across SDK versions/codecs while its logical rows are identical.
        existing = self.fetch_json(f"metrics/manifests/{batch_id}.json")
        if existing is not None:
            existing_manifest = MetricsManifest.from_dict(existing)
            if existing_manifest.batch_id != batch_id or existing_manifest.row_count != len(rows):
                raise ValueError(f"Metrics manifest mismatch for batch {batch_id}")
            self.read_metrics_batch(existing_manifest)
            return existing_manifest
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in rows:
            partition = metrics_partition(row.get("timestamp"))
            grouped.setdefault(partition, []).append(row)

        files: list[MetricsFileManifest] = []
        items: list[tuple[str, Any]] = []
        for (date, hour), partition_rows in sorted(grouped.items()):
            buffer = io.BytesIO()
            # Object dtype preserves nullable integer precision before Arrow encoding.
            pd.DataFrame(partition_rows, dtype=object).to_parquet(
                buffer, engine="pyarrow", compression="zstd", index=False
            )
            payload = buffer.getvalue()
            path = f"metrics/date={date}/hour={hour}/{batch_id}.parquet"
            files.append(
                MetricsFileManifest(
                    path=path,
                    date=date,
                    hour=hour,
                    row_count=len(partition_rows),
                    sha256=hashlib.sha256(payload).hexdigest(),
                )
            )
            items.append((path, io.BytesIO(payload)))

        manifest = MetricsManifest(
            version=1,
            batch_id=batch_id,
            row_count=len(rows),
            files=tuple(files),
        )
        # Keep this as one call: one HF commit, and manifest last for GCS.
        items.append((manifest.path, manifest.to_dict()))
        self.store_batch(items)
        stored = self.fetch_json(manifest.path)
        if stored is None:
            raise RuntimeError(f"Metrics manifest missing after write: {manifest.path}")
        stored_manifest = MetricsManifest.from_dict(stored)
        if stored_manifest != manifest:
            raise ValueError(f"Metrics manifest mismatch after write for batch {batch_id}")
        self.read_metrics_batch(stored_manifest)
        return stored_manifest

    def read_metrics_batch(self, manifest: MetricsManifest) -> list[dict[str, Any]]:
        """Read and fully validate every object referenced by the manifest."""
        if not HAS_PANDAS:
            raise ImportError("Pandas and pyarrow required for metrics reader")

        batch_rows: list[dict[str, Any]] = []
        for file in manifest.files:
            payload = self.fetch_binary(file.path)
            if payload is None:
                raise FileNotFoundError(f"Metrics object missing: {file.path}")
            try:
                parquet = payload.read()
            finally:
                payload.close()
            if hashlib.sha256(parquet).hexdigest() != file.sha256:
                raise ValueError(f"Metrics object digest mismatch: {file.path}")
            frame = pd.read_parquet(io.BytesIO(parquet), engine="pyarrow", dtype_backend="pyarrow")
            file_rows = _canonical_metric_rows(frame.to_dict(orient="records"))
            if len(file_rows) != file.row_count:
                raise ValueError(f"Metrics object row count mismatch: {file.path}")
            batch_rows.extend(file_rows)

        batch_rows = _canonical_metric_rows(batch_rows)
        if len(batch_rows) != manifest.row_count:
            raise ValueError(f"Metrics batch row count mismatch: {manifest.batch_id}")
        if _metric_rows_id(batch_rows) != manifest.batch_id:
            raise ValueError(f"Metrics batch identity mismatch: {manifest.batch_id}")
        return batch_rows

    def read_metrics(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        video_ids: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Read validated cold metrics and deduplicate by the hot-row key."""
        if not HAS_PANDAS:
            raise ImportError("Pandas and pyarrow required for metrics reader")

        rows: list[dict[str, Any]] = []
        for manifest_path in sorted(self.list_files("metrics/manifests/")):
            if not manifest_path.endswith(".json"):
                continue
            raw_manifest = self.fetch_json(manifest_path)
            if raw_manifest is None:
                raise FileNotFoundError(f"Metrics manifest missing: {manifest_path}")
            manifest = MetricsManifest.from_dict(raw_manifest)
            if manifest.path != manifest_path:
                raise ValueError(f"Metrics manifest path mismatch: {manifest_path}")
            if not any(
                (start_date is None or file.date >= start_date)
                and (end_date is None or file.date <= end_date)
                for file in manifest.files
            ):
                continue

            for row in self.read_metrics_batch(manifest):
                row_date, _ = metrics_partition(row.get("timestamp"))
                if start_date is not None and row_date < start_date:
                    continue
                if end_date is not None and row_date > end_date:
                    continue
                rows.append(row)

        deduplicated: dict[tuple[str, str], dict[str, Any]] = {}
        for row in _canonical_metric_rows(rows):
            video_id = str(row.get("video_id", ""))
            timestamp = str(row.get("timestamp", ""))
            if video_ids is not None and video_id not in video_ids:
                continue
            key = (video_id, timestamp)
            previous = deduplicated.get(key)
            if previous is not None and previous != row:
                raise ValueError(f"Conflicting metrics rows for {video_id}/{timestamp}")
            deduplicated[key] = row
        return list(deduplicated.values())


class HuggingFaceVault(VaultStrategy):
    def __init__(self) -> None:
        if not HAS_HF:
            raise ImportError(
                "HuggingFace dependencies not installed. Install with: pip install huggingface-hub"
            )
        if not settings.HF_DATASET_ID:
            raise ValueError("HF_DATASET_ID required for HuggingFace vault")

        self.repo_id = settings.HF_DATASET_ID
        self.token = settings.HF_TOKEN.get_secret_value() if settings.HF_TOKEN else None

        self.api = HfApi(token=self.token)

    def store_json(self, path: str, data: Any) -> None:
        try:
            json_bytes = json.dumps(data).encode("utf-8")
            self.api.upload_file(
                path_or_fileobj=io.BytesIO(json_bytes),
                path_in_repo=path,
                repo_id=self.repo_id,
                repo_type="dataset",
                commit_message=f"Vault: Add metadata {path}",
            )
            logger.info(f"Stored {path} to HF vault")
        except Exception as e:
            logger.exception(f"HF upload failed for {path}: {e}")
            raise

    @contextlib.contextmanager
    def _download_file(self, path: str, directory: Path | None = None) -> Iterator[Path]:
        """Scope downloaded bodies and Hub metadata to one read, then remove them."""
        if path.startswith("hf://"):
            path = path.split(self.repo_id + "/")[-1]
        with tempfile.TemporaryDirectory(prefix="pleiades-vault-", dir=directory) as folder:
            downloaded = hf_hub_download(
                repo_id=self.repo_id,
                filename=path,
                repo_type="dataset",
                token=self.token,
                local_dir=folder,
            )
            yield Path(downloaded)

    def fetch_json(self, path: str) -> dict[Any, Any] | None:
        try:
            with self._download_file(path) as local_path, local_path.open() as f:
                result: dict[Any, Any] = json.load(f)
                return result
        except EntryNotFoundError:
            logger.info(f"File not found in HF vault: {path}")
            return None
        except Exception as e:
            logger.exception(f"Failed to fetch {path} from HF vault: {e}")
            raise

    def list_files(self, prefix: str) -> list[str]:
        try:
            files = self.api.list_repo_files(
                repo_id=self.repo_id,
                repo_type="dataset",
            )
            return [f for f in files if f.startswith(prefix)]
        except Exception as e:
            logger.exception(f"Failed to list files with prefix {prefix}: {e}")
            return []

    def store_visual_evidence(
        self, video_id: str, frames: list[tuple[int, bytes]], ext: str = "webp"
    ) -> None:
        """Stores visual frames cleanly using a single commit operation to avoid API rate limits."""
        try:
            operations = []
            for idx, img_bytes in frames:
                path = f"frames/{video_id}/{idx}.{ext}"
                operations.append(CommitOperationAdd(path_in_repo=path, path_or_fileobj=img_bytes))

            if operations:
                self.api.create_commit(
                    repo_id=self.repo_id,
                    repo_type="dataset",
                    operations=operations,
                    commit_message=f"Vault: Visual Evidence {video_id} ({len(frames)} frames)",
                )
                logger.info(f"Archived {len(frames)} frames for {video_id} to HF")
            else:
                logger.warning(f"No frames provided to archive for {video_id}")
        except Exception as e:
            logger.exception(f"Failed to archive visuals for {video_id}: {e}")
            raise

    def store_visual_evidence_batch(
        self, entries: list[tuple[str, list[tuple[int, bytes]], str]]
    ) -> None:
        """Store keyframes for many videos in a single HF commit."""
        operations = []
        frame_count = 0
        for video_id, frames, ext in entries:
            for idx, img_bytes in frames:
                path = f"frames/{video_id}/{idx}.{ext}"
                operations.append(CommitOperationAdd(path_in_repo=path, path_or_fileobj=img_bytes))
                frame_count += 1
        if not operations:
            logger.warning("store_visual_evidence_batch: no frames provided")
            return
        try:
            self.api.create_commit(
                repo_id=self.repo_id,
                repo_type="dataset",
                operations=operations,
                commit_message=(
                    f"Vault: Visual Evidence batch ({len(entries)} videos, {frame_count} frames)"
                ),
            )
            logger.info(
                f"Archived {frame_count} frames for {len(entries)} videos to HF in one commit"
            )
        except Exception as e:
            logger.exception(f"Failed to archive visual batch to HF: {e}")
            raise

    def delete_files(self, paths: list[str]) -> int:
        """Delete the given repo-relative ``paths`` in batched commits (chunked
        to stay under HF's 128-commits/hour cap). Returns the number of files
        deleted."""
        if not paths:
            return 0
        total = 0
        chunk_size = 500
        for i in range(0, len(paths), chunk_size):
            batch = paths[i : i + chunk_size]
            ops = [CommitOperationDelete(path_in_repo=p) for p in batch]
            try:
                self.api.create_commit(
                    repo_id=self.repo_id,
                    repo_type="dataset",
                    operations=ops,
                    commit_message=f"Vault: purge {len(batch)} files",
                )
                total += len(batch)
                logger.info(f"Purged {len(batch)} files from HF vault")
            except Exception as e:
                logger.exception(f"HF purge failed: {e}")
                raise
        return total

    def store_batch(
        self, items: list[tuple[str, Any]], max_attempts: int = 5, base_delay: float = 10.0
    ) -> list[str]:
        """Write many files in a SINGLE commit (avoids HF's 128 commits/hour
        cap); retries internally with jittered exponential backoff.
        Returns vault URIs in input order.

        The previous defaults (8 attempts, 30s base, doubling to a 600s cap)
        could sleep 30+60+120+240+480+600+600 = ~35 minutes *inside one call*,
        and this runs in a worker thread. A second caller retry loop formerly
        pushed the worst case to ~1.8 hours of a
        parked thread per batch. Callers now run once on a dedicated executor;
        permanent errors are not retried.
        """
        if not items:
            return []
        delay = base_delay
        last_exc: Any = None
        for attempt in range(1, max_attempts + 1):
            try:
                operations = []
                for path, data in items:
                    if isinstance(data, io.BytesIO):
                        data.seek(0)
                        payload: Any = data
                    else:
                        payload = io.BytesIO(json.dumps(data).encode("utf-8"))
                    operations.append(
                        CommitOperationAdd(path_in_repo=path, path_or_fileobj=payload)
                    )
                self.api.create_commit(
                    repo_id=self.repo_id,
                    repo_type="dataset",
                    operations=operations,
                    commit_message=f"Vault: batch write ({len(items)} files)",
                )
                logger.info(f"Stored {len(items)} files to HF vault in one commit")
                return [f"hf://datasets/{self.repo_id}/{p}" for p, _ in items]
            except Exception as e:  # noqa: BLE001
                last_exc = e
                if _is_permanent(e):
                    logger.error(f"HF batch upload failed permanently, not retrying: {e}")
                    raise
                if attempt == max_attempts:
                    break
                if _is_rate_limited(e):
                    logger.warning(
                        f"Vault 429 (rate limited) - backing off (attempt {attempt}/{max_attempts})"
                    )
                else:
                    logger.warning(
                        f"HF batch upload failed (attempt {attempt}/{max_attempts}): {e}"
                    )
                _backoff_sleep(delay, attempt, max_attempts)
                delay = min(delay * 2, 120.0)
        if last_exc is None:  # pragma: no cover - loop always sets it
            raise RuntimeError("vault batch write made no attempt")
        raise last_exc

    def fetch_binary(self, path: str) -> io.BytesIO | None:
        try:
            with self._download_file(path) as local_path, local_path.open("rb") as f:
                return io.BytesIO(f.read())

        except Exception as e:
            logger.warning(f"Failed to fetch binary {path} from HF vault: {e}")
            return None

    def fetch_binary_to_path(self, path: str, destination: str | Path) -> bool:
        """Materialize media in temporary local storage, without a global copy."""
        try:
            destination = Path(destination)
            with self._download_file(path, destination.parent) as source:
                if source.is_symlink():
                    # Older Hub versions can link a local_dir file to the cache.
                    shutil.copyfile(source, destination)
                else:
                    source.replace(destination)
            return True
        except LocalEntryNotFoundError:
            # A failed local cache lookup is not evidence of a missing remote object.
            raise
        except EntryNotFoundError:
            return False
        except Exception as error:
            logger.warning("HF materialization failed (%s)", type(error).__name__)
            raise

    def append_metrics(
        self,
        data: list[dict[Any, Any]],
        date: str | None = None,
        hour: str | None = None,
    ) -> None:
        """Append time-series metrics as partitioned Parquet, writing each
        call's batch to its own timestamped file (avoids lost updates and
        O(n²) rewrites)."""
        if not data:
            logger.warning("No metrics data to append")
            return

        if not HAS_PANDAS:
            raise ImportError("Pandas required for metrics")

        if date is None:
            date = datetime.now(UTC).strftime("%Y-%m-%d")
        if hour is None:
            hour = datetime.now(UTC).strftime("%H")

        batch_ts = datetime.now(UTC).strftime("%H%M%S_%f")
        path = f"metrics/date={date}/hour={hour}/{batch_ts}.parquet"

        try:
            buffer = io.BytesIO()
            new_df = pd.DataFrame(data)
            new_df.to_parquet(buffer, engine="pyarrow", index=False)
            buffer.seek(0)

            self.api.upload_file(
                path_or_fileobj=buffer,
                path_in_repo=path,
                repo_id=self.repo_id,
                repo_type="dataset",
                commit_message=f"Append metrics: {len(data)} rows to {path}",
            )

            logger.info(f"Appended {len(data)} metrics to {path}")

        except Exception as e:
            logger.exception(f"Failed to append metrics to {path}: {e}")
            raise


class GCSVault(VaultStrategy):
    def __init__(self) -> None:
        if not HAS_GCS:
            raise ImportError(
                "Google Cloud Storage not installed. Install with: pip install google-cloud-storage"
            )
        if not settings.GCS_BUCKET_NAME:
            raise ValueError("GCS_BUCKET_NAME required for GCS vault")

        self.bucket_name = settings.GCS_BUCKET_NAME
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)

    def store_json(self, path: str, data: Any) -> None:
        try:
            blob = self.bucket.blob(path)
            blob.upload_from_string(json.dumps(data), content_type="application/json")
            logger.info(f"Stored {path} to GCS vault")
        except Exception as e:
            logger.exception(f"GCS upload failed for {path}: {e}")
            raise

    def fetch_json(self, path: str) -> dict[Any, Any] | None:
        try:
            blob = self.bucket.blob(path)
            if not blob.exists():
                return None
            result: dict[Any, Any] = json.loads(blob.download_as_text())
            return result
        except Exception as e:
            logger.exception(f"Failed to fetch {path} from GCS vault: {e}")
            raise

    def list_files(self, prefix: str) -> list[str]:
        try:
            blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
            return [blob.name for blob in blobs]
        except Exception as e:
            logger.exception(f"Failed to list files with prefix {prefix}: {e}")
            return []

    def store_visual_evidence(
        self, video_id: str, frames: list[tuple[int, bytes]], ext: str = "webp"
    ) -> None:
        """Stores visual frames individually using the frames/ path."""
        try:
            for idx, img_bytes in frames:
                path = f"frames/{video_id}/{idx}.{ext}"
                blob = self.bucket.blob(path)
                blob.upload_from_string(img_bytes, content_type=f"image/{ext}")
            logger.info(f"Stored {len(frames)} frames for {video_id} to GCS")
        except Exception as e:
            logger.exception(f"Failed to store visuals for {video_id}: {e}")
            raise

    def store_visual_evidence_batch(
        self, entries: list[tuple[str, list[tuple[int, bytes]], str]]
    ) -> None:
        """Store keyframes for many videos (GCS: one upload per blob)."""
        try:
            count = 0
            for video_id, frames, ext in entries:
                for idx, img_bytes in frames:
                    path = f"frames/{video_id}/{idx}.{ext}"
                    blob = self.bucket.blob(path)
                    blob.upload_from_string(img_bytes, content_type=f"image/{ext}")
                    count += 1
            logger.info(f"Stored {count} frames for {len(entries)} videos to GCS")
        except Exception as e:
            logger.exception(f"Failed to store visual batch to GCS: {e}")
            raise

    def delete_files(self, paths: list[str]) -> int:
        if not paths:
            return 0
        count = 0
        for p in paths:
            blob = self.bucket.blob(p)
            if blob.exists():
                blob.delete()
                count += 1
        logger.info(f"Purged {count} files from GCS vault")
        return count

    def store_batch(
        self, items: list[tuple[str, Any]], max_attempts: int = 5, base_delay: float = 5.0
    ) -> list[str]:
        """Write many files in one logical batch (GCS: individual blob uploads).

        Retries internally on HTTP 429 (GCS per-project write cap). The attempt
        budget is deliberately small: this runs in a worker thread, and a long
        back-off here parks that thread for the whole pipeline. See
        ``HuggingFaceVault.store_batch`` for the same reasoning.
        """
        if not items:
            return []
        delay = base_delay
        last_exc: Any = None
        for attempt in range(1, max_attempts + 1):
            try:
                uris = []
                for path, data in items:
                    if isinstance(data, io.BytesIO):
                        data.seek(0)
                        blob = self.bucket.blob(path)
                        blob.upload_from_file(data)
                    else:
                        blob = self.bucket.blob(path)
                        blob.upload_from_string(json.dumps(data), content_type="application/json")
                    uris.append(f"gs://{self.bucket_name}/{path}")
                logger.info(f"Stored {len(items)} files to GCS vault")
                return uris
            except Exception as e:  # noqa: BLE001
                last_exc = e
                if _is_permanent(e):
                    logger.error(f"GCS batch upload failed permanently, not retrying: {e}")
                    raise
                if attempt == max_attempts:
                    break
                if _is_rate_limited(e):
                    logger.warning(
                        f"GCS 429 (rate limited) - backing off (attempt {attempt}/{max_attempts})"
                    )
                else:
                    logger.warning(
                        f"GCS batch upload failed (attempt {attempt}/{max_attempts}): {e}"
                    )
                _backoff_sleep(delay, attempt, max_attempts)
                delay = min(delay * 2, 120.0)
        if last_exc is None:  # pragma: no cover - loop always sets it
            raise RuntimeError("vault batch write made no attempt")
        raise last_exc

    def fetch_binary(self, path: str) -> io.BytesIO | None:
        try:
            if path.startswith("gs://"):
                path = path.split(self.bucket_name + "/")[-1]

            blob = self.bucket.blob(path)
            if not blob.exists():
                return None

            buffer = io.BytesIO()
            blob.download_to_file(buffer)
            buffer.seek(0)
            return buffer
        except Exception as e:
            logger.warning(f"Failed to fetch binary {path} from GCS vault: {e}")
            return None

    def fetch_binary_to_path(self, path: str, destination: str | Path) -> bool:
        """Download a GCS object directly to *destination*."""
        try:
            if path.startswith("gs://"):
                path = path.split(self.bucket_name + "/")[-1]

            blob = self.bucket.blob(path)
            if not blob.exists():
                return False
            blob.download_to_filename(str(destination))
            return True
        except Exception as error:
            logger.warning("GCS materialization failed (%s)", type(error).__name__)
            raise

    def append_metrics(
        self,
        data: list[dict[Any, Any]],
        date: str | None = None,
        hour: str | None = None,
    ) -> None:
        """Append time-series metrics as partitioned Parquet, writing each
        call's batch to its own timestamped file (avoids lost updates and
        O(n²) rewrites)."""
        if not data:
            logger.warning("No metrics data to append")
            return

        if not HAS_PANDAS:
            raise ImportError(
                "pandas required for metrics append. Install: pip install pandas pyarrow"
            )

        if date is None:
            date = datetime.now(UTC).strftime("%Y-%m-%d")
        if hour is None:
            hour = datetime.now(UTC).strftime("%H")

        batch_ts = datetime.now(UTC).strftime("%H%M%S_%f")
        path = f"metrics/date={date}/hour={hour}/{batch_ts}.parquet"

        try:
            buffer = io.BytesIO()
            new_df = pd.DataFrame(data)
            new_df.to_parquet(buffer, engine="pyarrow", index=False)
            buffer.seek(0)

            blob = self.bucket.blob(path)
            blob.upload_from_file(buffer, content_type="application/octet-stream")

            logger.info(f"Appended {len(data)} metrics to {path}")

        except Exception as e:
            logger.exception(f"Failed to append metrics to {path}: {e}")
            raise


_vault_instance: VaultStrategy | None = None


def get_vault() -> VaultStrategy:
    """Get or create the vault singleton.

    Instantiation is deferred until the first call so that importing
    ``atlas.vault`` does not trigger environment-variable validation or
    network calls (useful for testing).
    """
    global _vault_instance
    if _vault_instance is None:
        _vault_instance = GCSVault() if settings.VAULT_PROVIDER == "gcs" else HuggingFaceVault()
    return _vault_instance


def audio_path(video_id: str, chunk: int | None = None) -> str:
    """Shard new audio writes to avoid the Hub's per-directory file limit."""
    # Keep new objects outside the already-full legacy audio directory.
    base = f"media/audio/{artifact_shard(video_id)}/{video_id}"
    return f"{base}.opus" if chunk is None else f"{base}/{chunk:03d}.opus"


def artifact_shard(video_id: str) -> str:
    """Return the stable shard used by per-video cold-storage artifacts."""
    return video_id[:2] if len(video_id) >= 2 else video_id


def raw_path(video_id: str, filename: str) -> str:
    """Return a sharded path for a newly fetched raw media artifact."""
    return f"raw/{artifact_shard(video_id)}/{filename}"


def transcript_index_path(video_id: str) -> str:
    """New heads live beside immutable bodies; legacy bodies stay untouched."""
    return transcript_path(video_id).removesuffix(".json") + "/index.json"


def transcript_path(video_id: str) -> str:
    """Return the sharded repo-relative vault path for a video's transcript.

    Transcripts are sharded by the first two chars of the video ID so that no
    single directory exceeds HuggingFace's 10k-entries-per-directory limit
    (e.g. ``transcripts/ab/<id>.json``).
    """
    return f"transcripts/{artifact_shard(video_id)}/{video_id}.json"


def meta_path(video_id: str) -> str:
    """Return the sharded path for newly stored yt-dlp metadata."""
    return f"meta/{artifact_shard(video_id)}/{video_id}.info.json"


def legacy_meta_path(video_id: str) -> str:
    """Return the pre-sharding metadata path retained for existing vault rows."""
    return f"meta/{video_id}.info.json"


def video_path(video_id: str, ext: str = "mp4") -> str:
    """Return the repo-relative vault path for a video's archived source clip."""
    return f"videos/{video_id}.{ext}"


def __getattr__(name: str) -> Any:
    """Lazy module attribute for backward-compatible ``from atlas.vault import vault``."""
    if name == "vault":
        return get_vault()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


if TYPE_CHECKING:
    vault: VaultStrategy
