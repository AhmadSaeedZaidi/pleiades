# Reusable hot/cold storage

A small Python 3.12 library with **no runtime dependencies** and no Pleiades,
YouTube, PostgreSQL, Prefect, or credential imports. Install this directory in
another project, or build its wheel:

```bash
python -m pip install /path/to/pleiades/tiered_storage
python -m pip wheel --no-deps /path/to/pleiades/tiered_storage -w dist
```

## Working example

```python
import asyncio
from tiered_storage import FilesystemColdStore, SQLiteHotStore, TieredStorage

async def main():
    hot = SQLiteHotStore("data/hot.db", retention_seconds=0)
    cold = FilesystemColdStore("data/cold")
    storage = TieredStorage(hot, cold)
    await hot.stage("article/42", b"Example article")
    assert await storage.read("article/42") == b"Example article"
    result = await storage.flush()
    print(result.promoted)
    assert await storage.read("article/42") == b"Example article"

asyncio.run(main())
```

For a persistent service, set `retention_seconds` to how long payloads should
remain hot, and schedule `flush()` yourself. The package starts no workers,
servers, or scheduler. An executable [example](examples/local_storage.py) accepts
a data directory and defaults to a temporary directory.

## Handoff contract

```mermaid
sequenceDiagram
    participant Hot
    participant Mover
    participant Cold
    Mover->>Hot: Select eligible key + version + bytes
    Mover->>Cold: Write immutable objects in one bounded batch
    Mover->>Cold: Read receipts and verify SHA-256
    Mover->>Hot: Transactionally finalize matching versions
    Hot-->>Mover: Completed keys; changed versions stay hot
```

`TransferPolicy` defaults to 50 records, 16 MiB of payload bytes per write,
and four concurrent readbacks. Oversized records fail explicitly and remain hot.
These limits bound writes and verification; selection adapters must also avoid
loading an unbounded result set. Cold adapters may deduplicate identical bodies.

Writes, readbacks, or finalization failures preserve retryable hot state.
`TransferResult` reports promoted keys, deferred keys changed during the handoff,
and failures containing the key, stage, and exception class. It never includes
exception messages or payloads. Cancellation propagates without pretending to
roll back a remote write. Cold objects can remain orphaned after interruption;
retries use the same content-addressed paths. This is an at-least-once handoff,
not a distributed transaction or a cross-process lease.

SQLite finalizes one batch in a transaction using key/version/digest checks.
The filesystem adapter writes through temporary files, fsyncs, and atomically
replaces the destination before returning its URI. It rejects reads outside
its configured root. Use a trusted local directory; this is not a security
boundary against another process modifying directory symlinks concurrently.
Clearing payloads frees logical database space; physically shrinking an existing
SQLite file is a separate maintenance decision. Garbage collection and backups
remain application responsibilities.

For staged rows that already have a durable cold copy, set `existing_uri` on
`StagedItem`. The mover verifies it and skips uploading if its digest matches.
A missing or different copy is replaced at a new immutable path before hot
finalization. Existing URIs must refer to stable content; an application should
never overwrite objects that finalized receipts reference.

## Other backends

The reusable unit is this package, not Atlas as a whole. Version 0.1 is a small
handoff engine with a tested local backend, rather than a complete storage
service. Its wheel works in a clean Python environment without Atlas, Maia,
Prefect or Hugging Face installed, including reads after a process restart.

| Ready here | Supplied by the integrating project |
| --- | --- |
| Digest verification and version-aware finalization | Backend-specific transactions and durability guarantees |
| SQLite hot storage and filesystem cold storage | PostgreSQL, Hugging Face, GCS or other SDK adapters |
| Bounded writes, bounded readback concurrency and retry-safe immutable paths | Scheduling, distributed leases, observability, backups and cold-object cleanup |

Payloads are byte arrays in memory; there is no streaming API. The byte limit
applies to transfer batches, not the entire memory footprint of selected rows.
Choose selection limits for your record sizes. Adapter correctness matters:
the engine cannot make an overwrite-prone cold backend immutable or an
unconditional hot deletion version-safe. There is no cross-backend distributed
transaction, automatic failover, or packaged remote-backend SDK integration.

Implement the exported `HotStore` and `ColdStore` protocols:

| Adapter | Methods | Required behavior |
| --- | --- | --- |
| Hot | `pending(limit)`, `finalize(items)`, `get(key)` | Bounded selection; conditional transactional finalization; payload or cold URI plus digest |
| Cold | `write_batch(items)`, `read(uri)` | Immutable idempotent writes; exact key-to-URI receipts; bytes readback |

Construct `StagedItem(key, version, payload, namespace="documents/42", suffix=".json")`.
Its path contains the content digest. `StoredItem` passes the original version
and verified URI to hot finalization. Keep domain schemas and SQL in the adapter.
If only archival is needed, use `promote(items, cold, finalize, policy)` directly
without implementing a read API. Backend SDK retries belong inside the adapter;
the engine makes one attempt per operation and leaves later retries to scheduling.

The [Pleiades integration](../docs/tiered-storage.md) uses the same engine with
PostgreSQL repositories and the existing Hugging Face/GCS vault.
