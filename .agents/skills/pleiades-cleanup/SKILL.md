---
name: pleiades-cleanup
description: Audit and remove obsolete Pleiades code, docs, skills, tests, and workflows while preserving the running extraction pipeline.
---

# Pleiades cleanup

Use the [architecture skill](../pleiades-architecture/SKILL.md) and read [current audit](../../../docs/audits/OCTOBER_CLEANUP.md) plus
[cleanup ledger](../../../docs/implementation-checklists/cleanup-plan.md). Older audits/worklogs are
historical evidence, not instructions to repeat completed changes.

Preserve unrelated dirty-tree changes. Before deleting a subsystem, trace its
runtime imports, CLI/Prefect entrypoints, tests, build files and docs. Remove its
dependencies and callers together, then run the canonical gate. Retired training
and prediction modules are not part of the active YouTube extraction product.

For behavior changes, add focused failure/durability tests before refactoring.
Keep useful core regressions; remove tests whose only subject is removed code.
Avoid maintaining duplicate test fixtures or weakening assertions for test counts.

Consolidate current operating guides instead of adding another plan or worklog.
Check relative links with `make docs-check`. Keep only repository-specific skills;
generic cloud/ML/database instructions belong in the user's global skill library.

The live database is read-only during cleanup. Schema/lease changes require
reviewed migration files and explicit production deployment, not automatic DDL.
Do not stop/restart ingestion or use live integration runs as a unit-test gate.
Never print or commit secrets, cookies, exception payloads, or downloaded media.

Verify with `make check`, bounded read-only runtime observations, and browser
checks for UI changes. Report source changes versus live activation accurately.
Leave changes reviewable; do not commit unrelated work or auto-merge/deploy.
