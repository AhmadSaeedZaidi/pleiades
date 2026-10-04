# YouTube topic knowledge graph

The graph connects the existing video and channel collection to the Wikipedia
article URLs reported by YouTube's `topicDetails.topicCategories`. These are
broad classifications, such as Science, Music, or Technology. They are not
transcript facts, a Wikipedia crawl, or a complete semantic graph. The deprecated
Freebase topic IDs are not used. See the official
[video resource](https://developers.google.com/youtube/v3/docs/videos#topicDetails) and
[channel resource](https://developers.google.com/youtube/v3/docs/channels#topicDetails).

## Collection and storage

Normal video/channel metadata requests include `topicDetails` alongside their
existing parts. Atlas commits the resource and its observed topic relationships
in the same transaction. Search snippets and statistics-only responses do not
clear known topics. Wikipedia URLs are canonicalized and deduplicated without
fetching arbitrary remote pages.

The additive [migration](../deploy/sql/20261004_knowledge_graph.sql) creates one
topic dictionary, video/channel junction tables, and an enrichment queue in the
existing PostgreSQL database. The `wiki_topics` video field is kept as a
convenient projection and included in cold metadata snapshots. Relationships
remain available when Janitor archives a video; deleting the resource cascades
to its graph edges. Unreferenced dictionary entries may remain for history.

Each topic edge has an observation time and the provenance
`youtube.topicDetails.topicCategories`; publishing edges use
`youtube.snippet.channelId`. A channel does not inherit its videos' topics.
Related topics in a view share sampled videos; no topic-to-topic facts are invented.

## Bounded backfill

The ninth scheduler operation runs every ten minutes. A cycle services both
videos and channels, with at most four requests of fifty IDs each. It uses the
hunting key pool, leaving tracking capacity protected. The Data API list methods
cost one quota unit per request; retries can consume additional quota. New
metadata requests collect topics without an additional request.

Queue claims use owned ten-minute leases and `SKIP LOCKED`. API calls never
hold row locks. Completion accepts only the active unexpired owner; a newer
ingestion snapshot cancels an older backfill claim. Successful observations
refresh after thirty days. Known-empty resources are counted separately;
unavailable resources retain their last known facts and retry after seven days.
Request errors retry after an hour. Rate limits remain handled by the shared
YouTube executor; different keys need not imply independent project quota.

Set `TOPIC_SYNC_ENABLED=false` to disable backfill. A manual bounded cycle is:

```bash
python -m maia.topics --batch-size 50 --max-batches 4
```

This calls YouTube and writes the configured database. It is an operator command,
not a test or browser endpoint. Full coverage develops gradually; the UI reports
checked resources and unavailable/empty outcomes rather than claiming completion.

## Read-only explorer

Open **Knowledge graph** in the dashboard. Search Wikipedia topics, select one,
inspect a video, highlight connections, or export the current JSON view. All
reads use the dashboard's separate read-only pool and statement timeout. The
browser never invokes agents or consumes YouTube quota.

`GET /api/graph?q=Science&limit=25` returns a bounded projection. `topic` can
select a canonical Wikipedia URL. Search is capped at 120 characters; the view
caps at 50 videos, 20 directly classified channels, and 12 related topics.
Exports identify `pleiades.topic-graph.v1`, `sampled: true`, and whether the view
uses demo data. They contain included nodes and provenance-bearing edges, not
the whole database.

## Activation and verification

Apply only the reviewed additive migration to an existing installation; do not
rerun the full legacy provisioning script as a migration. Install source changes,
run `make check`, and restart ingestion and the separate dashboard service after
applying the migration. Source files alone do not activate an already-running
Python process. Unit coverage is in Atlas, Maia, and dashboard; real database
regressions live in guarded Alkyone storage tests and run against an ephemeral
PostgreSQL instance in CI.
