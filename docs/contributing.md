# Contributing

Use the shared Python 3.12 environment or a separate development environment.
Read [AGENTS.md](../AGENTS.md), [architecture](architecture.md), and
[testing](testing.md) before changing pipeline behavior.

Preserve existing worktree changes. Add focused behavior tests before changing
failure/claim/persistence semantics. Keep SQL in Atlas repositories and call
plain operations from Maia. Avoid new orchestration/deployment dependencies for
small UI or application changes.

Run `make check`. Keep infrastructure tests in guarded Alkyone suites. A local
test gate must not mutate production, download media, send Discord messages, or
consume paid transcription quota.

Update the relevant current guide when behavior changes. Historical audits and
migration notes retain their dates and do not serve as the active work ledger.
Do not commit `.env`, cookies, downloaded transcripts, or run logs.

Leave live service restarts and database migrations for an explicit deployment
with reviewed changes. Describe what changed, what was verified, and what still
needs production activation in the pull request.
