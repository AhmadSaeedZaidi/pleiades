# Configuration and secrets

Credentials live in gitignored `.env`, cookie files, or the deployment secret
store. This tracked document contains configuration names only.

| Configuration | Purpose |
| --- | --- |
| `DATABASE_URL` | Pipeline PostgreSQL connection |
| `YOUTUBE_API_KEY_POOL_JSON` | Data API key pool |
| `HF_DATASET_ID`, `HF_TOKEN` | Artifact vault location and token |
| `YOUTUBE_COOKIES_PATH` | Local YouTube cookie file |
| `MISTRAL_API_KEY`, `GROK_API_KEY` | Optional transcription/summary providers |
| `DISCORD_WEBHOOK_*` | Operator notifications |
| `PLEIADES_DASHBOARD_TOKEN` | Optional dashboard API access |
| `PLEIADES_DASHBOARD_DATABASE_URL` | Optional SELECT-only dashboard connection |

Use [Atlas examples](atlas/ENV.example), [Maia examples](maia/ENV.example),
and [dashboard setup](dashboard/README.md) for configuration. Private SSH keys,
internal host details, cookie expiry notes, and token values do not belong here.
