# Orchestrator Contract — `maia/src/maia/orchestrator.py`

Status: **current contract** (grounded in scheduler source and unit tests).
Scope: cleanup-plan item 3(a). Companion tests: `maia/tests/test_orchestrator.py`.

---

## 1. Role & dual-server division

The orchestrator is the **in-process scheduler** for the nine-agent Pleiades fleet. It
replaces the old `prefect worker start --type process` model, which spawned a full
`python -m prefect.engine` subprocess (~17 threads, 170–400 MB RSS) per flow run and blew
the systemd `TasksMax` ceiling.

**Dual-server division** (see `docs/micro-prefect-orchestration.md` §1, §8):

| Host | Role | Runs |
|------|------|------|
| **Control plane (micro)** `e2-micro-server` | Optional Prefect **server/API** on `:4200` (SQLite) | stores deployment metadata and optional telemetry — **not** video data or work state |
| **Executor VPS** `10.0.0.6` | `pleiades-ingestion.service` running `maia.orchestrator` in-process | drives nine scheduled application operations on one asyncio loop; Archeologist stays manual-only |

The live scheduler does not require `PREFECT_API_URL` or a remote Prefect run. The
`@flow` entrypoints remain optional rollback/flow-level telemetry adapters; the
orchestrator owns cadence and execution while PostgreSQL-backed repositories own
durable work state. The orchestrator never talks to the video DB directly.

---

## 2. Public surface

| Symbol | Kind | Signature | Returns |
|--------|------|-----------|---------|
| `CycleSpec` | `@dataclass` | `(name: str, operation: CoroFactory, interval: float, kwargs: dict = {}, initial_jitter: float = 0.0)` | — |
| `CoroFactory` | type alias | `Callable[..., Any]` | a coroutine |
| `build_specs()` | function | `() -> list[CycleSpec]` | the nine agent specs |
| `run_cycle(name, operation, *, kwargs=None, jitter=0.0)` | `async` | `(str, CoroFactory, dict | None, float) -> None` | `None` |
| `agent_loop(spec)` | `async` | `(CycleSpec) -> None` | never returns (infinite) |
| `run(specs=None)` | `async` | `(list[CycleSpec] \| None) -> None` | never returns (infinite) |
| `main()` | function | `() -> None` | `None` |
| `_run_until_stop(stop)` | `async` | `(asyncio.Event) -> None` | `None` |

`build_specs()` defines the production cadence and invokes plain operations;
`prefect.yaml` contains only unscheduled compatibility adapters:

| name | operation | interval (s) | kwargs | initial jitter (s) |
|------|-----------|--------------|--------|-------------------|
| streamer | `streamer_operation` | 120 | `{"batch_size": 5}` | 0.0 |
| singer | `singer_operation` | 300 | `{"batch_size": 10}` | 0.6 |
| painter | `painter_operation` | 120 | `{"batch_size": 5}` | 1.2 |
| scribe | `scribe_operation` | 120 | `{"batch_size": 10}` | 1.8 |
| hunter | `hunter_operation` | 1200 | `{"batch_size": 1}` | 2.4 |
| tracker | `tracker_operation` | 60 | `{"batch_size": 50}` | 3.0 |
| heartbeat | `heartbeat_operation` | 900 | `{}` | 3.6 |
| janitor | `janitor_operation` | 900 | `{"dry_run": False}` | 4.2 |
| grapher | `grapher_operation` | 600 | `{"batch_size": 50, "max_batches": 4}` | 4.8 |

---

Actual cycle starts, finishes, cancellations and failures are observed through
`maia.telemetry.CycleMonitor`. This bounded process-local history adds no work
state or scheduling. Cadence ages are measured from completion, avoiding false
late warnings for normal sleep after a long cycle. The [heartbeat](../heartbeat.md)
consumes these observations; they reset on restart.

## 3. Responsibilities (owned by the orchestrator)

1. **Scheduling loop** — `agent_loop` runs one agent forever: `run_cycle(...)` then
   `asyncio.sleep(spec.interval)`. The interval is measured between cycle attempts;
   a slow cycle cannot overlap with its own next attempt.
2. **Worker dispatch** — `run` creates exactly **one asyncio task per spec**
   (`name=f"cycle-{spec.name}"`) and `asyncio.gather`s them all on the single loop.
3. **Failure isolation** — `run_cycle` wraps the awaited coroutine in
   `try/except Exception` and **logs** (`orchestrator cycle <name> FAILED`) instead of
   raising. A single failing cycle must never stall or kill the loop.
4. **Cancellation pass-through** — `asyncio.CancelledError` is **re-raised** (not
   swallowed) so shutdown can cancel in-flight plain operations. `_run_until_stop`
   awaits the cancelled runner before returning.
5. **Jitter stagger** — `build_specs()` assigns explicit first-tick jitter values
   so the fleet does not fire all at once after a restart. Jitter applies **only to
   the first tick**; subsequent ticks sleep the plain interval.
6. **Signal drain** — `main` installs `SIGINT`/`SIGTERM` handlers that set a stop event;
   `_run_until_stop` cancels the runner task; `main` then cancels all pending tasks and
   drains them with `asyncio.gather(..., return_exceptions=True)` before closing the loop.
7. **Error/retry policy** — the orchestrator does **no retry**. A failed cycle is logged
   and the next cycle runs at the next interval. Retry/backoff is delegated to the agents
   (e.g. hunter/archeologist swallow YouTube daily-quota errors and retry next interval;
   streamer backs off via `atlas.state`).

### Observed cycle states

Scheduling still follows the same loop. The cycle monitor records observations
without owning work, retries or persistence:

| State | Meaning |
| --- | --- |
| starting | registered, awaiting its first attempt |
| running | a plain operation is executing |
| idle | the previous cycle completed and the loop is waiting |
| failed | an exception, partial failure, or Discord non-delivery was observed |
| late | a cycle has not started within its completion-based cadence and grace |
| slow | running longer than the configured reporting threshold |
| interrupted | cancellation was propagated without recording success |

Quota pauses remain separate signals owned by the agents. Previous failure
counts remain visible during a retry and reset on success. These observations
reset when the process restarts and never initiate or cancel work.

---

## 4. NON-responsibilities (delegated to other modules)

The orchestrator **does not**:

- **Touch the DB** — it never opens a connection or runs SQL. All persistence is via the
  agents' repositories (`atlas.repositories.*`), which the orchestrator only invokes
  through the plain operations. (Tests therefore mock operation callables; no
  FakeDriver/DB is needed at the orchestrator layer.)
- **Manage the watchlist** — `WatchlistRepository` tier decay / `calculate_next_track_time`
  / `update_schedule` are owned by the **tracker**.
- **Stage transcripts** — owned by the **scribe**.
- **Write to the vault** — owned by the **janitor** (`vault_flush_task`).
- **Report fleet health** — Heartbeat owns collection and reporting. The
  orchestrator observes its cycles through `CycleMonitor` and schedules
  `heartbeat_operation` as one of its nine scheduled cycles.
- **Enforce per-agent concurrency** — the scheduler owns one non-overlapping loop per
  agent and BaseBatchAgent owns its internal item semaphore. Prefect work-queue limits
  apply only when a rollback adapter is deliberately run through Prefect.
- **Retry** — see §3.7.

---

## 5. Interaction with the DB / collaborators

The orchestrator's collaborators are the nine scheduled operation callables and
the process-local cycle monitor. It calls
`spec.operation(**spec.kwargs)` and awaits the resulting coroutine. It holds **no
repository, no client, no connection**. This makes it trivially unit-testable: mock the
operations (return `AsyncMock` coroutines) and assert scheduling/dispatch/isolation
behavior without any DB. There is no FakeDriver at this layer because the orchestrator
never reaches the DB — the FakeDriver convention applies to the agents' repository tests.

---

## 6. Test contract (what `test_orchestrator.py` must prove)

1. `build_specs()` returns exactly the nine agents with the documented intervals/kwargs.
2. `run_cycle` logs completion on success, swallows+logs a generic failure, re-raises
   `CancelledError`, and sleeps `jitter` before awaiting when jitter is nonzero.
3. `agent_loop` calls the plain operation with the spec kwargs, sleeps the interval
   after each cycle, and applies explicit jitter only on the first tick.
4. `run` dispatches exactly one task per spec with `cycle-<name>` names.
5. `_run_until_stop` cancels and awaits the runner once the stop event is set.
6. `main` installs SIGINT/SIGTERM handlers and drains pending tasks on shutdown.
