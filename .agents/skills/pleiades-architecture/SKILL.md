---
name: pleiades-architecture
description: Repository-specific ownership and test conventions for Pleiades pipeline, vault, and dashboard changes.
---

# Pleiades architecture

Read [architecture](../../../docs/architecture.md) for the current data flow and [testing guide](../../../docs/testing.md) for
verification. Source packages use src-layout: Atlas owns repositories/storage,
Maia owns plain agent operations, dashboard owns a read-only web service, MCP
owns on-demand tools, and Alkyone owns guarded integration/live scenarios.
The independent `tiered_storage` package owns verified handoffs and local
SQLite/filesystem adapters; it must never import Atlas, Maia, or environment settings.

## Running system

`pleiades-ingestion.service` runs `maia.orchestrator` with nine plain operations.
Prefect adapters remain compatibility entrypoints; they do not own cadence.
Archeologist and full-clip capture are manual. Do not stop or restart the existing
service, change its environment, or write to its database during cleanup.

## Persistence invariants

- SQL belongs in `atlas.repositories`. DashboardRepository accepts a separate
  read-only pool; do not use the write-capable global DatabaseManager for UI reads.
- Use the generic write/verify/version-conditional handoff for staged storage.
  Backend SQL and vault adapters stay in Atlas; domain code stays out of the reusable package.
- Scribe stages transcripts in PostgreSQL. Janitor is the single transcript-vault
  writer, batching commits and finalizing non-empty pointers atomically.
- Vault handles its own 429 retries. Run sync vault work in the bounded vault
  executor; do not add a second caller retry loop or starve media work.
- Tracking uses durable watchlist samples and survives Janitor archival.
- Per-stage failures and lifecycle status are distinct. Preserve stage-specific
  release/completion guards; row locks are not durable owner leases.
- Keep tracker quota capacity protected. Inspect `atlas/key_pool.py` and config
  rather than assuming key rotation creates independent project quota.
- Invoke yt-dlp using `sys.executable -m yt_dlp`.

## Tests and verification

`make check` runs lint/types, unit suites, JS syntax, and active documentation
links. Tests mock database/vault/network/notification collaborators. The shared
`unit_test_guard` fixture blocks unmocked PostgreSQL calls in unit suites.
Mock event emission on failure paths. Test plain operations or Prefect task `.fn`
bodies, using Maia's logger/context fixtures when necessary.

Keep unit regressions in the owning component. Alkyone integration/live tiers
have separate explicit guards; do not run them against production during cleanup.
Do not hardcode historical test counts or claim local changes are live deployments.
