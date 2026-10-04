# Discord heartbeat

The application scheduler runs Heartbeat every 15 minutes after the preceding
report finishes. It reads local observations and bounded PostgreSQL snapshots;
the webhook is its only normal outbound request. It does not probe speech APIs,
fetch vault objects, enrich topics, or call YouTube. Optional Prefect telemetry
is collected only through the compatibility flow.

The report includes:

- Executor liveness and actual states of all nine scheduled workers, including
  Topics. Failed, interrupted, late, and unusually long cycles are flagged.
- Extraction totals, recent ingestion, per-stage failures and legacy failed records.
- Tracking updates, durable due work and SQL metric rows.
- SQL size, staged and retained transcript payloads, verified handoffs and metric
  archival observed by this process, plus host disk usage and free space.
- Wikipedia topic/link counts, checked video/channel coverage, recent checks,
  unavailable resources, and whether enrichment is enabled.
- Configured transcription provider and recorded quota pauses. Configuration
  is not proof that a credential or endpoint works.

**Healthy** means the available observations have no flagged issues.
**Attention** means failures, quota pauses, missing metrics, late work or disk
usage at least 85%. **Degraded** means the executor is unavailable or disk usage
is at least 95%. `executor_online` reports liveness independently of `healthy`;
old failed records remain visible without being presented as a new incident.
Optional Prefect states do not determine live scheduler health.

Cycle observations use a bounded in-memory history, reset on process restart,
and explicitly label progress as the last hour or the time since startup.
Counts reflect successful handoffs/archival, not attempted batch sizes.
Exceptions are recorded by class, without payloads or private endpoint details.
The current attempt stays visible alongside previous failures until recovery.
Missing collectors show **unavailable**, rather than fabricated zero counts.
Quota reporting ignores expired marks without rewriting other agents' shared
state; unreadable state is surfaced without silencing the report.
Running the standalone Heartbeat CLI cannot observe the scheduler in another
process and reports that limitation.

Each SQL collector has an eight-second deadline including pool acquisition;
graph and storage queries also use a five-second SQL statement timeout. The
collectors run concurrently. Discord delivery has a ten-second total timeout;
non-delivery is recorded as a failed heartbeat cycle for the next report.
Fields are bounded to fit [Discord embed limits](https://docs.discord.com/developers/resources/message#embed-limits).

Late detection allows the configured interval plus at least 60 seconds of grace
after completion. Running work is marked slow after the greater of 15 minutes
and twice its cadence; it is not forcibly cancelled by reporting. The reported
collection duration helps judge overhead. Logical hot-body removal does not
imply that PostgreSQL's physical files have shrunk.

This heartbeat shares the scheduler process. A stopped host, dead process or
blocked event loop cannot send its own outage report; detecting whole-host
silence requires an external watchdog.
