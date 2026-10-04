# API and network resilience

`YOUTUBE_API_KEY_POOL_JSON` supplies the key pool. `settings.key_rings` partitions
it into hunting, tracking, and archeology rings. Dynamic allocation uses corpus
size while preserving tracking capacity. Key quotas can share a project-level
budget; rotating keys does not guarantee independent quota.

`KeyRing.from_keys` is the supported constructor for an explicitly supplied pool.
Resiliency executors classify Data API failures and rotate exhausted keys without
silently retrying application validation errors. Quota exhaustion is an operator
signal distinct from local proxy failure or permanently unavailable media.

The media path uses yt-dlp through `sys.executable -m yt_dlp`, configured cookies,
Deno, and egress settings. Vault writes run in a dedicated bounded executor;
media work has separate capacity. Vault batches handle Hugging Face 429 backoff
internally; callers must not multiply those retries.

Local state suppresses repeated alerts and expires quota/backoff marks. It is an
operational hint, not durable database work ownership. Do not call state-writing
helpers from a read-only dashboard.

[Operations](deploy.md) · [Storage](tiered-storage.md)
