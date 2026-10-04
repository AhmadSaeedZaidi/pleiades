# Hot and cold storage

PostgreSQL holds work state, metadata, durable tracking state, recent metrics,
and staged transcripts. The Hugging Face/GCS vault holds media, transcripts,
metadata snapshots, and cold metrics. The reusable
[tiered storage package](../tiered_storage/README.md) owns the handoff algorithm;
Atlas owns PostgreSQL SQL and the vault adapter, and Maia schedules the work.
The independent package also provides SQLite/filesystem adapters and transparent
hot/cold reads, without requiring any pipeline dependencies or credentials.

## Verified handoffs

Scribe locks the parent video, stages transcript JSON, and queues a vault write.
Janitor selects a bounded batch, writes immutable content-addressed bodies,
verifies their SHA-256 digests, and finalizes matching PostgreSQL row versions in
one transaction. Changed versions remain hot for the next cycle. No database
lock is held over network I/O. Metadata archival uses the same engine, then
rechecks lifecycle, row version, pending flags, and transcript safety under the
parent lock before marking videos archived and deleting transcript rows.
Recent metric samples and durable tracking state remain hot.

A transcript flush uses one commit for up to 50 records/16 MiB, rather than a
fixed 25-record chunk. Oversized individual records remain hot and are reported
as failures. Readbacks run with bounded concurrency on Atlas's separate
2-thread vault executor. Backends own their retries; the caller has no second
retry loop. Python still waits for active worker threads at process exit.

New paths:

```text
transcripts/{id_prefix}/{id}/{sha256}.json
transcripts/{id_prefix}/{id}/index.json
metadata/{date}/{id}/{sha256}.json
metadata/{date}/{id}/index.json
media/audio/{id_prefix}/{id}.opus
media/audio/{id_prefix}/{id}/{chunk}.opus
metrics/date={date}/hour={hour}/{batch_id}.parquet
metrics/manifests/{batch_id}.json
```

Small JSON indexes point to immutable bodies and include their digest. Vault
readers prefer the new index and retain compatibility with sharded/flat legacy
bodies. Legacy body paths are preserved, so existing finalized URIs stay stable.
Metadata snapshots retain the transcript URI before the hot transcript row is
removed. Purge helpers collect both legacy paths and all versioned objects.
Index heads are convenience lookups; Janitor remains their single scheduled
writer. Immutable receipts and conditional finalization make retries safe, but
do not replace durable leases or cross-process ordering of index updates.

Audio writes use shards because the live flat directory reached the
[Hub's 10,000-file limit](https://huggingface.co/docs/hub/storage-limits).
Readers and purge helpers also support existing flat audio; no bulk move or
deletion is needed. Legacy nullable metric counters encoded as doubles are
normalized back to integers, then checked against the original logical batch
digest. Precision loss still fails verification and keeps the hot rows.

## Retained transcript payloads

The October audit found about 57,500 rows with both a vault pointer and retained
JSON, contributing to a transcript table of about 1.5 GB including indexes and
TOAST. These are candidates for reconciliation, not proof that every remote copy
is valid. A small live read-only sample matched its cold copies.

Janitor prioritizes newly staged transcripts, then selects a bounded batch of
retained payloads. It canonicalizes legacy JSON and checks the existing cold
copy. Matching copies need no upload; missing/different copies are written and
verified at a new immutable path. Only then does a version-conditional batch
transaction clear hot content. Rows without a hot payload are never replaced
with JSON null merely to clear a pending flag.

This releases logical hot payloads gradually. PostgreSQL autovacuum can reuse
freed pages; an existing table's physical files do not automatically shrink.
No full-table rewrite, `VACUUM FULL`, arbitrary retention deletion, or automatic
cold-object garbage collection is part of this change.

## Metrics and indexes

Metric archival keeps two days of detail hot by default, independently of the
seven-day processed-video retention policy. It processes at most eight 5,000-row
batches per cycle, then leaves
remaining work for later cycles. It selects each batch oldest first, uploads/reads back a
manifest-backed Parquet batch, and deletes only matching `(video_id, timestamp,
row_version)` tuples. A metric changed during upload stays hot. Completed batch
retries reuse the existing manifest and skip Parquet serialization; logical
identity is independent of codec/version-specific file bytes. New files use
Zstandard compression. Nullable large integers retain precision through object
construction and Arrow-backed reads.

[The index migration](../deploy/sql/20261004_tiered_storage_indexes.sql) adds a
`(timestamp, video_id)` retention index and targeted transcript/pending indexes.
The retention query previously scanned/sorted roughly 900,000 rows on this
instance. Run the migration outside a transaction and verify all indexes have
`indisvalid = true`; a failed concurrent build can leave an invalid index that
`IF NOT EXISTS` will not repair. Fresh schema setup includes equivalent indexes.

A local 5,000-row representative sample compares Snappy and Zstandard in the
current audit. Measurements describe that sample, not a guaranteed production
compression ratio or end-to-end speedup. No partitioning rewrite is needed for
the current table size.

## Verification and operations

`make check` covers the package and application unit suites. Janitor dry runs
skip all mutation phases and persisted events; they do not flush hot payloads. Separately,
`make -C alkyone test-storage` runs DB-only concurrency/transaction regressions
against positively identified isolated PostgreSQL. It uses the existing Alkyone
production guards, needs no genuine YouTube/HF credentials, and is never part of
the live unit-test gate. See [testing](testing.md).

Installing/building the package or passing local tests does not reload the
running ingestion process. See [deployment](deploy.md) and the
[current audit](audits/OCTOBER_CLEANUP.md) for recorded activation status.

PostgreSQL documents [row version identities](https://www.postgresql.org/docs/16/ddl-system-columns.html)
and [locking/isolation behavior](https://www.postgresql.org/docs/16/transaction-iso.html).
Here `xmin` is a short-lived handoff token, never a permanent globally unique
identifier. Pandas documents [supported Parquet compression](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_parquet.html).

## Disk budgets and reconciliation

`JANITOR_METRICS_RETENTION_DAYS` defaults to two days. The durable watchlist keeps
the last tracking sample even when older detailed metrics move to compressed
Parquet in the vault. This limits hot history without restarting discovery or
losing tracking cadence. Increase the setting if longer immediate SQL history
is useful; cold metric manifests remain the historical source.

Transcript staging has no age-based loss policy: a body stays in SQL until its
cold copy verifies and conditional finalization succeeds. Reconciliation now
uses an independent budget of ten batches of one hundred transcripts per
fifteen-minute Janitor cycle, with a 120-second soft elapsed budget checked
between batches. Configure `JANITOR_TRANSCRIPT_BATCH_SIZE`,
`JANITOR_TRANSCRIPT_MAX_BATCHES`, and `JANITOR_TRANSCRIPT_BUDGET_SECONDS`.
The handoff also caps each upload batch at 16 MiB and uses the shared bounded
vault executor. A failed batch stops that cycle rather than reselecting the same
failure repeatedly. Existing verified pointers are reused before uploading.

Hugging Face reads use temporary `local_dir` downloads. Media moves into the
caller's working directory; JSON and byte reads release their downloaded files
on return. Completed and failed reads remove Hub metadata and temporary bodies,
so the global cache no longer accumulates another local copy of vault contents.
The tradeoff is additional downloads for repeated reads. Current locked Hub
versions support this behavior; older versions may still use a linked cache.

PostgreSQL deletion releases logical payloads before it releases filesystem
space. Ordinary `VACUUM (ANALYZE)` makes dead space reusable and refreshes planner
statistics; it is not a promise to shrink relation files. Table rewrites need
extra free disk and locks. Inspect measured sizes and existing maintenance before
choosing compaction. Never delete SQL payloads solely because a vault URI exists.

The reviewed [per-table maintenance migration](../deploy/sql/20261004_storage_maintenance.sql)
lowers vacuum/analyze scale factors to 2% for transcripts and detailed metrics,
including transcript TOAST payloads. This makes released space reusable sooner
without changing global server settings. It preserves rows and is separate from
one-time operator `VACUUM (ANALYZE)` maintenance.
