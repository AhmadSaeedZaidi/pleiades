# Operations and deployment

The current application is one `pleiades-ingestion.service` running
`python -m maia.orchestrator`. The versioned definition is in
[deploy/systemd](../deploy/systemd/pleiades-ingestion.service).
A separate [YouTube proxy unit](../deploy/systemd/youtube-proxy.service) provides
the configured egress path. Prefect is optional compatibility infrastructure.

Read-only observations:

```bash
systemctl status pleiades-ingestion --no-pager
systemctl show pleiades-ingestion -p ActiveState -p SubState -p NRestarts -p MemoryCurrent
journalctl -u pleiades-ingestion -n 80 --no-pager
make dashboard
```

Inspect logs locally; they can contain video titles, exception text, or URLs.
Never paste secrets into issues or the web UI. Scheduler liveness, quota
exhaustion, and artifact completeness are separate signals.

Development dashboard sessions bind to loopback on port 8080. The persistent
server deployment uses a separate user service and the `/pleiades/` route in
the existing Nginx server, redirecting to a password-protected Caddy HTTPS proxy
on port 8443. The [dashboard guide](../dashboard/README.md) includes
the service, proxy configuration, and SSH access alternative. Dashboard restarts
do not restart ingestion.

GitHub CD is triggered by successful CI on main. It checks out the CI-verified
SHA, refuses local modifications on the target checkout, installs dependencies,
and runs the canonical gate before restarting the configured service. Manual
workflow dispatch remains an explicit operator action. The workflow requires
the `DEPLOY_HOST`, `DEPLOY_USER`, `DEPLOY_SSH_KEY`, `DEPLOY_PATH`, and optional
`INGESTION_SERVICE` repository secrets. Never run it to clean a dirty live tree.

Docker's default command runs the same single scheduler. The existing per-agent
Compose definitions are for isolated/manual operation; do not launch them beside
systemd against the same database because stages still lack durable owner leases.

Schema migration SQL is stored in [deploy/sql](../deploy/sql/20260902_claim_indexes_v2.sql).
Review locks, timeouts, preflight counts, backup, rollback, and the actual live
schema before applying any migration. Source DDL is not proof a migration ran.
The dashboard uses PostgreSQL read-only sessions and applies no migrations.

The additive [storage indexes](../deploy/sql/20261004_tiered_storage_indexes.sql)
run outside a transaction. Install `tiered_storage` before Atlas in pip-based
deployments (`make install` and the Dockerfiles handle this order). Local path
dependencies are recorded in the Poetry locks. Storage code changes require an
ingestion restart after the canonical gate; schema index creation alone does not
activate the new handoff logic.

The [topic graph migration](../deploy/sql/20261004_knowledge_graph.sql) is additive
and transactional, with short lock and statement timeouts. Apply it before
activating the topic collector or graph API. Backfill is bounded and gradual;
see [knowledge graph operations](knowledge-graph.md).

CI runs the canonical gate, ephemeral PostgreSQL regressions, and a production
image build. It installs from committed package locks and needs no GHCR bootstrap
image or production credentials. The Alkyone external-infrastructure workflow
remains manual and guarded.
