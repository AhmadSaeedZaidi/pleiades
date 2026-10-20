# Repository guidance

Read `.agents/skills/pleiades-architecture/SKILL.md` when changing pipeline code.
For audit/cleanup work, also read `.agents/skills/pleiades-cleanup/SKILL.md`.
These are repository-specific skills; generic cloud/ML skills are not vendored here.

- Preserve unrelated changes in this dirty working tree.
- The live `pleiades-ingestion.service` and its environment are running production.
  Do not stop/restart them, migrate the database, or run mutating live tests during cleanup.
- Pipeline PostgreSQL SQL belongs in `atlas.repositories`; agents call plain async
  operations. The independent `tiered_storage` package owns its SQLite adapter
  and must not import pipeline settings or packages.
- Run `make check`. Unit tests must mock repositories, vaults, and outbound notifications.
  Alkyone integration/live tests have separate fail-closed guards and opt-ins.
- Dashboard queries use their own read-only pool, bounded pages and query timeouts.
  Never make browser requests call agent operations or paid APIs.
- Never print secrets, commit `.env`/cookies, or put exception payloads in the UI.
- Source changes require deployment to take effect in the existing Python process.
  Report this distinction; do not claim that a successful local check changed live behavior.
