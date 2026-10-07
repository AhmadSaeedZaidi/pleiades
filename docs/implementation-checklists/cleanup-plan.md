# Cleanup ledger

Updated 2026-10-07. Supersedes the historical item-by-item worklogs.

Completed in the October cleanup: removed the unused training/prediction stack,
retired its workflows/tests, consolidated current guides and repository skills,
hardened unit DB isolation, added a read-only UI, and included it in the quality
gate. See [current audit](../audits/OCTOBER_CLEANUP.md) for validation evidence.

Janitor's source-retirement changes are reviewable: repeated missing-ID Tracker
observations park unfetched sources without deleting artifacts or resetting phases,
and successful rechecks restore eligibility. See [storage](../tiered-storage.md).
The reviewed migration and ingestion/dashboard deployment remain separate from
source verification; the live system is unchanged by local tests.

Remaining coordinated work:

- Durable, owner-specific work leases; prove stale workers cannot finalize new claims.
- Normalize redundant video flags/phases and transcript/raw staging with reviewed migrations.
- Reconcile source DDL with the actual live schema, including claim indexes.
- Remove Prefect adapters only after replacing their actual MCP/CLI/integration consumers.
- Extract a YouTube core to reduce the MCP's dependency on pipeline configuration.
- Govern project-level API spend and reconcile all egress/error classifications.
- Review bulk recovery/migration utilities before production use.

During cleanup the live database is read-only. Do not apply migrations, restart
ingestion, or run integration tests against production. There is no requirement
to preserve obsolete wrappers merely because tests still reference them; migrate
the real callers and behavior tests together before deleting them.
