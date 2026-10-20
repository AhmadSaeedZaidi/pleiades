# Historical Prefect topology

The live scheduler is now `maia.orchestrator` under
`pleiades-ingestion.service`. Prefect adapters remain for compatibility; deploying
or starting a Prefect worker is not required to run ingestion.

Use [operations and deployment](deploy.md) for current commands and
[architecture](architecture.md) for ownership. The small secondary VPS still
provides an independent network exit where configured. It does not own cadence.
