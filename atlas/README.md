# Atlas

The infrastructure layer: typed video/channel/watchlist models, PostgreSQL
repositories, artifact vault, API key resilience, events, and notifications.
Source lives in `src/atlas`; unit tests live in `tests`.

SQL belongs to repositories. `VideoRepository` composes ingestion, tracking,
state, retention, quality, and transcript collaborators. `TranscriptRepository`
owns DB staging; Janitor owns cold persistence. `WatchlistRepository` owns durable
last-sample tracking and schedule state. `DashboardRepository` accepts an isolated
read-only pool and never calls the write-capable pipeline database manager.

Atlas validates pipeline configuration at import time. Use gitignored `.env` or
export the variables documented in [ENV.example](ENV.example). Hugging Face and
GCS are optional storage extras; install `[hf]` for the current deployment.

```bash
make -C atlas test-unit
make -C atlas lint
```

`schema.sql` describes source DDL. `atlas.setup` applies it and is a mutating
operation, never a read-only preflight. Review production changes as versioned
migration SQL. See [architecture](../docs/architecture.md),
[storage](../docs/tiered-storage.md), and [testing](../docs/testing.md).
