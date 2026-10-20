# Pleiades

A running YouTube extraction pipeline: discover videos, capture source media,
extract audio/keyframes/transcripts, track engagement, and persist artifacts.
PostgreSQL owns work state; a Hugging Face vault stores large artifacts.

## Operator interface

```bash
make dashboard        # http://127.0.0.1:8080, reads the configured pipeline
make dashboard-demo   # explicitly labeled sample data, no external calls
```

The interface provides collection metrics, hourly discovery activity, extraction
coverage, search and filters, video inspection, staged transcript previews,
discovery queries, recent persisted events, and a Wikipedia topic graph explorer. It runs separately from ingestion
and uses a two-connection, read-only PostgreSQL pool. See
[dashboard setup and access](dashboard/README.md).

## Components

| Directory | Responsibility |
| --- | --- |
| [atlas](atlas/README.md) | PostgreSQL repositories, models, vault, network resilience |
| [tiered_storage](tiered_storage/README.md) | Reusable, dependency-free hot/cold handoffs and local storage adapters |
| [maia](maia/README.md) | Agent operations and the single application scheduler |
| [dashboard](dashboard/README.md) | Operator UI and read-only API |
| [mcp](mcp/README.md) | Optional on-demand tools for MCP clients |
| [alkyone](alkyone/README.md) | Guarded integration and live preflight tests |
| [deploy](docs/deploy.md) | Reviewed service definitions and migration SQL |

The active scheduler is `python -m maia.orchestrator`, managed by
`pleiades-ingestion.service`. It calls plain async operations. Prefect adapters
remain for compatibility; Prefect does not own live cadence. Archeologist and
full-clip capture are manual capabilities.

## Development

Use Python 3.12, ffmpeg, and Deno for the media path. Never replace the running
service's environment or apply the schema as part of a local cleanup.

```bash
make install         # install component dependencies in your development environment
make check           # Ruff, formatting, types, hermetic tests, active doc links
make test-unit       # unit suites only; no live services or API quota
make test-int        # separately configured and guarded Alkyone integration tests
```

[Quick start](docs/quickstart.md) · [Architecture](docs/architecture.md) ·
[Operations](docs/deploy.md) · [Tests](docs/testing.md) ·
[Current audit](docs/audits/OCTOBER_CLEANUP.md)

The discontinued ML training/prediction stack and its workflows have been removed.
Historical findings are evidence, not current operating instructions. See
[documentation index](docs/README.md) for current guides.

Licensed under [MIT](LICENSE.md).
