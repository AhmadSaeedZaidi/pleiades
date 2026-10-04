# Maia

Agent operations for the YouTube pipeline. The live process is
`python -m maia.orchestrator` under `pleiades-ingestion.service`.

| Operation | Cadence after each cycle | Batch |
| --- | --- | --- |
| Streamer: source media | 120 seconds | 5 |
| Singer: audio | 300 seconds | 10 |
| Painter: keyframes | 120 seconds | 5 |
| Scribe: transcripts | 120 seconds | 10 |
| Hunter: discovery | 1,200 seconds | 1 |
| Tracker: engagement | 60 seconds | 50 |
| Heartbeat: operator reporting | 900 seconds | — |
| Janitor: persistence/retention | 900 seconds | operation-specific |

Cadences include operation runtime. Archeologist and Muralist/full clips remain
manual. The scheduler calls plain `*_operation` functions and isolates cycle
failures. Prefect adapters remain for compatibility, without scheduling work.

Agents call Atlas repositories and storage/network collaborators. Vault operations
use a bounded executor; media subprocesses require ffmpeg, Deno and configured
YouTube egress/cookies. Scribe stages transcripts; Janitor commits them.

```bash
make -C maia test-unit
make -C maia lint
```

For standalone CLI arguments, run `python -m maia --help`. Do not run competing
scheduler/agent processes against the live database while auditing.
See [architecture](../docs/architecture.md), [operations](../docs/deploy.md), and
[tests](../docs/testing.md).
