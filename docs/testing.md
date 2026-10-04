# Testing

`make check` is the canonical gate: Ruff, formatting, type checks, hermetic unit
tests, dashboard JavaScript syntax, and current documentation links.
`make test-unit` runs tiered storage, Atlas, Maia, MCP, the Alkyone safety guard, and dashboard
unit suites. Root `pytest` also defaults to these unit paths.

Unit tests inject fake collaborators. The root `conftest.py` prevents accidental
pipeline database access. Each component runs with dummy credentials; MCP tests
override inherited credentials instead of retaining production environment
values. Mock event emission, vault methods, and notifications when exercising
agent failure paths. Test plain `*_operation` functions or `.fn` task bodies;
avoid launching Prefect infrastructure merely to test application behavior.

```bash
make test-unit
make lint
make docs-check
.venv/bin/python -m pytest atlas/tests -q
.venv/bin/python -m pytest maia/tests -q
make -C dashboard test
```

Alkyone component tests use real, isolated PostgreSQL/vault/API infrastructure.
`make test-int` is opt-in and refuses known production targets before collection.
Keep integration scenarios there; unit regressions belong to the owning component.
Live preflight checks use a separate explicit opt-in (`make test-live`) and remain
read-only. Do not run either tier as part of ordinary cleanup.

The DB-only storage suite is a separate isolated tier:

```bash
make -C tiered_storage test
make -C alkyone test-storage
```

For `test-storage`, set `ALKYONE_ALLOW_INTEGRATION=1`, a dedicated `DATABASE_URL`
matching `ALKYONE_TEST_DATABASE_URL`, and the production references required by
Alkyone's existing guard. It creates/truncates its test tables, so use a disposable
UTF-8 database on a separate PostgreSQL instance. Set `HF_DATASET_ID` and
`ALKYONE_TEST_VAULT` to the same isolated placeholder; no remote vault calls occur.
Use dummy `HF_TOKEN` and YouTube keys with a distinct production key reference.
Do not provide live cookies. The suite verifies actual SQL row-version guards,
late Scribe rejection, retained recent metrics, and failed/changed handoffs.

CI runs the same gates on application changes and builds the production image.
Alkyone integration tests run by manual dispatch with test infrastructure.
Deployment listens for successful CI completion on main and selects that exact
commit. The remote deployment refuses a dirty checkout and runs `make check`
before a restart. A successful local gate is not a production deployment.

Keep tests that verify failure isolation, atomic database changes, durability,
bounds, and access controls. Remove tests with retired subsystems rather than
maintaining unused dependencies or weakening assertions to preserve a count.
