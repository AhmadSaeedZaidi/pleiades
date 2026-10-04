# Documentation

Current guides:

- [Quick start](quickstart.md): development environment and first checks.
- [Architecture](architecture.md): ownership, data flow, scheduler, and components.
- [Operations and deployment](deploy.md): live service observation and reviewed deployments.
- [Heartbeat](heartbeat.md): Discord health, cycle progress and monitoring limits.
- [Knowledge graph](knowledge-graph.md): observed YouTube Wikipedia classifications and bounded enrichment.
- [Dashboard](../dashboard/README.md): access, configuration, and limits.
- [Testing](testing.md): hermetic unit tests and guarded integration/live tiers.
- [Adaptive tracking](adaptive-scheduling.md): durable watchlist and cadence.
- [Storage](tiered-storage.md): hot PostgreSQL and the artifact vault.
- [API resilience](resiliency-strategy.md): key rings and quota handling.
- [Contributing](contributing.md): change and verification workflow.
- [Current audit](audits/OCTOBER_CLEANUP.md): verified findings and remaining work.

[Scheduler contract](implementation-checklists/orchestrator-contract.md) records
behavior covered by unit tests. [Cleanup ledger](implementation-checklists/cleanup-plan.md)
records current remaining work.

The challenges log, vault migration notes, adaptive-scheduling implementation
checklist, and September audits are historical records. Dates, test counts, and
commands there describe earlier versions; use the guides above for current work.
