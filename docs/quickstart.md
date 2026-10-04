# Quick start

Use Python 3.12 and Node for JavaScript syntax checks. On the existing executor,
use the shared `.venv` for checks and
leave its running process/environment intact. On a new development checkout:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install poetry
make install
make check
make dashboard-demo
```

The media pipeline needs ffmpeg and Deno. The dashboard demo needs neither a
media runtime nor credentials. Open `http://127.0.0.1:8080`.

For a real dashboard, use the pipeline's existing gitignored `.env`, or export
`DATABASE_URL` and the Atlas configuration documented in
[atlas/ENV.example](../atlas/ENV.example). Run `make dashboard` from the root.
See [dashboard configuration](../dashboard/README.md) for access tokens and a
separate database URL.

Provision a separate development database before running ingestion on a fresh
machine. `atlas.setup` writes schema; it is not a read-only health check and must
never be pointed at production as part of setup verification.

[Testing](testing.md) covers isolated infrastructure. [Operations](deploy.md)
covers the existing systemd service and deployments.
