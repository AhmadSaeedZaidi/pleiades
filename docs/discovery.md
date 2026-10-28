# Discovery balance

Hunter explores curated seed topics and bounded tags from newly admitted videos.
Automated discovery no longer treats repeated tags as unlimited evidence of
relevance. Region balancing uses channel `snippet.country` metadata, never an
inference from a title, language, name or ethnicity. Missing or unrecognized
countries use a separate unknown bucket.

## Population is a prior

Population is not YouTube audience size, creator count or video supply. The
checked-in [population snapshot](../atlas/src/atlas/data/population_2024.json)
uses World Bank WDI `SP.POP.TOTL`, year 2024, retrieved on 2026-10-20. Its source URL
and retrieval date are recorded in the file. Aggregates are excluded. See the
[World Bank API documentation](https://datahelpdesk.worldbank.org/knowledgebase/articles/889392-about-the-indicators-api).

Country prior weights are `population ** DISCOVERY_POPULATION_EXPONENT`, then
normalized with water-filling under the country cap, reserving the unknown share.
The default exponent is 0.5: a country with 100 times the population receives ten
times the prior weight, before capping. Setting the exponent to zero gives equal
country priors; one gives population-proportional priors before capping. These
are explicit collection-policy choices, not estimates of national YouTube use.
Supported search markets without a population observation receive an explicit
neutral country prior; they are not mislabeled as unknown or assigned a
fabricated population.

The sampler compares both lifetime and trailing-seven-day country shares with
the prior. Above-prior candidates receive deterministic thinning and negative
query/tag priority. The stronger of lifetime and recent pressure applies, with a
small balanced pseudo-count for startup. Admission is stable for identical IDs
and observations; replaying a page does not provide another random draw.

A separate trailing-seven-day country cap defaults to 10%, and the unknown
budget to 5%. Once the recent cohort has at least one hundred observations,
over-cap countries cannot contribute new IDs until their share falls. Accepted
candidates consume the page's budget immediately. This prevents overshooting
inside one fifty-result page. Existing IDs remain refreshable but cannot seed
the snowball again; the insertion transaction returns the authoritative novelty
result, including when discovery producers run concurrently.

## Preventing feedback loops

- Search mention influence saturates at ten instead of growing indefinitely.
- A query has a twelve-hour cooldown between acknowledged pages.
- Twenty-four broad seed topics compete with tags, preventing regional keyword
  clusters from owning the entire queue. Curated seeds retain their explicit
  priority and cooldown state; learned penalties and Janitor culling cannot reset
  them into an immediate retry loop. Configure `DISCOVERY_SEED_TERMS` to
  reflect the intended corpus rather than treating these defaults as its purpose.
- Only new, geographically admitted and successfully inserted videos seed tags.
  Each contributes at most three normalized tags; a page contributes at most
  twenty unique terms. Geographic penalties follow the source into term priority.
- Exploration markets are selected using weighted deficits and the versioned
  list of supported YouTube search markets, not a country blacklist. Countries
  with no matching supply do not permanently monopolize exploration.
- Balanced mode searches relevance-ranked first pages over a seven-day window.
  Pagination tokens are not reused across changing market contexts.

The API's [`regionCode`](https://developers.google.com/youtube/v3/docs/search/list)
controls where a result is viewable; it does not prove the publisher's location.
Country admission uses resolved channel metadata separately. Quality-gate
channel enrichment supplies that metadata without an extra request; if quality
gating is disabled, the country lookup still runs. Metadata/SQL failures fail the
page rather than bypassing balancing. Country metadata itself can be absent or
self-declared, so this policy is not proof of cultural or linguistic diversity.

## Controls and observations

Defaults are `DISCOVERY_BALANCE_ENABLED=true`,
`DISCOVERY_POPULATION_EXPONENT=0.5`, `DISCOVERY_MAX_COUNTRY_SHARE=0.10`,
`DISCOVERY_UNKNOWN_COUNTRY_SHARE=0.05`, `DISCOVERY_PENALTY_WEIGHT=30`,
`DISCOVERY_QUERY_COOLDOWN_HOURS=12`, `DISCOVERY_LOOKBACK_DAYS=7`,
`DISCOVERY_MAX_TAGS_PER_VIDEO=3`, `DISCOVERY_MAX_TERMS_PER_PAGE=20`, and
`DISCOVERY_MENTION_CAP=10`. Source/configuration changes need a reviewed
process restart. Balancing also applies to Archeologist's historical discovery;
explicit on-demand lookups remain operator-directed.

Heartbeat's **Discovery balance** field shows the largest recent country share,
the prior and cap, unknown metadata coverage, and observed admitted/held counts.
Concentration remains an attention condition while the existing recent cohort
is imbalanced. Its SQL snapshot is read-only, bounded by a five-second statement
timeout and the heartbeat's collector deadline. No population or market data is
fetched at runtime.

The graph explorer also interleaves country cohorts in its bounded sample,
labeling the view and export rather than hiding the corpus-wide counts.
The admission patch changes future discovery; it does not delete videos or graph edges,
relabel existing facts, or make the historical graph instantly representative.
Geographic caps can reduce ingestion volume when discovery cannot find diverse
eligible sources. Review observed yields and the intended topic/language scope
before interpreting the corpus as a representative sample.
