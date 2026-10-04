# Operator dashboard

```bash
make dashboard       # live configured database; http://127.0.0.1:8080
make dashboard-demo  # sample data, clearly labeled; no external calls
```

This service provides an overview, hourly discovery chart, extraction coverage,
searchable/paginated video library, per-video phase and tracking details, staged
transcript previews, discovery queue, and persisted event log. Auto-refresh polls
every 30 seconds while visible; overview queries are cached for 15 seconds.

The frontend uses local HTML/CSS/JavaScript assets. There is no Node dependency
installation, CDN, frontend build, or second UI framework. Node is used only for
JavaScript syntax validation. `make -C dashboard install` installs the small
FastAPI service in a development environment.

## Connection and access

The existing pipeline `.env` is loaded without overriding exported variables.
Live mode imports Atlas and therefore uses the configured pipeline environment.
The demo does not import pipeline configuration or require credentials.

| Variable | Behavior |
| --- | --- |
| `DATABASE_URL` | Existing pipeline PostgreSQL URL |
| `PLEIADES_DASHBOARD_DATABASE_URL` | Optional separate URL, preferably a SELECT-only role |
| `PLEIADES_DASHBOARD_TOKEN` | Optional bearer token required by every API route |
| `PLEIADES_DASHBOARD_DEMO=1` | Explicitly enable sample data |

Bind defaults to `127.0.0.1:8080`. From a workstation:

```bash
ssh -L 8080:127.0.0.1:8080 ubuntu@YOUR_EXECUTOR
```

Open `http://127.0.0.1:8080` locally. If a token is configured, enter it in the UI;
it stays in browser memory, never in query strings or local storage. Use TLS
when transmitting an access token. The public hobby deployment exposes only
the existing read-only, redacted metadata API; it has no browser write controls.

CLI: `python -m pleiades_dashboard --host 127.0.0.1 --port 8080 [--demo]`.
Run from the root with `PYTHONPATH=dashboard/src:atlas/src` when not installed.

## Persistent password-protected hosting

The server address is `https://opencodeserver.duckdns.org:8443/pleiades/`.
The port-80 `/pleiades/` route redirects there. Username is `operator`; the
password is supplied privately by the operator, never stored in Git. Port 443
belongs to another service on this host.

Two persistent user units run the loopback dashboard and its Caddy HTTPS proxy:
[dashboard](../deploy/systemd/pleiades-dashboard.service) and
[proxy](../deploy/systemd/pleiades-dashboard-proxy.service). Install Caddy,
then generate a bcrypt password hash with `caddy hash-password`. Put only the
hash in `.secrets/dashboard-proxy.env` as
`PLEIADES_DASHBOARD_PASSWORD_HASH=...` (directory mode 700, file mode 600).
The directory is Git-ignored. The [Caddyfile](../deploy/caddy/Caddyfile) protects
the page, assets, and every API endpoint with HTTP Basic authentication over TLS.
The backend binds to `127.0.0.1:8080`.

```bash
mkdir -p ~/.config/systemd/user
cp deploy/systemd/pleiades-dashboard*.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now pleiades-dashboard pleiades-dashboard-proxy
systemctl --user status pleiades-dashboard pleiades-dashboard-proxy
```

User lingering must be enabled for startup at boot. The
[Nginx route](../deploy/nginx/pleiades.conf) belongs inside the existing port-80
server block and preserves other sites. It forwards only the ACME HTTP challenge
to the proxy's port 8090 on the local Docker bridge; the dashboard route redirects
to HTTPS. Adapt the bridge, domain, paths, and ports for another server. Allow
TCP 8443 publicly and TCP 8090 only from the Docker bridge subnet. Port 80 must
remain reachable for certificate issuance and renewal.

Caddy strips `/pleiades`; `--root-path /pleiades` keeps assets, API requests, and
optional token checks consistent. `/healthz` checks process liveness, not database
or ingestion health. The hosted deployment uses proxy password authentication;
the optional API bearer token remains useful for private direct deployments.
Do not configure a backend token with this proxy without also arranging its
upstream authorization header.

The [topic graph](../docs/knowledge-graph.md) adds a read-only explorer and bounded
JSON exports over YouTube’s observed Wikipedia classifications.

## Operational limits

All API routes are reads. PostgreSQL sessions enforce read-only transactions,
5-second statement timeouts, 1-second lock timeouts, and at most two connections.
Search is parameterized; stage/status filters are allowlisted; pages cap at 50
rows and page 1,000. Detail reads cap transcript previews and metric samples.
Event payloads, database exception text, credentials, and vault/local paths are
excluded. Queries do not run agents or paid API calls.

Service status probes `pleiades-ingestion` where systemd is available. Unknown
means the probe is unavailable; event freshness does not substitute for liveness.
Coverage reflects hot stage flags and can decrease after archival. Transcript
previews show staged PostgreSQL content; already-vaulted content is not fetched.
Discovery buckets are UTC; other timestamps follow the browser's locale.

`make -C dashboard test` checks filters, pagination, access control, error
redaction, query caching, read-only pool configuration, and API bounds.
