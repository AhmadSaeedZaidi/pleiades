# Alkyone

Guarded pipeline integration and read-only live preflight tests. Per-component
unit tests belong to Atlas, Maia, MCP, and dashboard, not this package.

```bash
make -C alkyone test-unit  # safety guard tests; hermetic
make test-int             # isolated real infrastructure; explicit opt-in
make test-live            # separately opted-in read-only live preflight
```

Component tests require a non-production database and test vault. The guard
runs before test collection and refuses known production targets. Live tests
have a separate explicit opt-in and validate configuration/connectivity without
mutating the running database or consuming extraction quota.

Inspect `src/alkyone/guard.py`, the component/live conftests, and the Makefile for
exact environment requirements. Never bypass a guard to make a test run.
See [testing](../docs/testing.md) for the full policy.
