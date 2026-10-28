from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from atlas.repositories.knowledge_graph import KnowledgeGraphReader, graph_projection
from atlas.topics import resource_topics, wikipedia_topic
from atlas.youtube import lookup_channels, lookup_videos


@pytest.mark.parametrize(
    "url",
    [
        "http://en.wikipedia.org/wiki/Science#History",
        "https://en.m.wikipedia.org/wiki/Science?oldid=1",
        "https://en.wikipedia.org/wiki/%53cience",
    ],
)
def test_topic_urls_have_one_canonical_identity(url):
    topic = wikipedia_topic(url)
    assert topic.url == "https://en.wikipedia.org/wiki/Science"
    assert (topic.label, topic.language) == ("Science", "en")


def test_unicode_topics_preserve_language_and_readable_label():
    topic = wikipedia_topic("https://fr.wikipedia.org/wiki/Caf%C3%A9_du_monde")
    assert topic.label == "Café du monde"
    assert topic.language == "fr"
    assert topic.url.endswith("Caf%C3%A9_du_monde")


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/wiki/Science",
        "https://en.wikipedia.org.evil/wiki/Science",
        "https://user@en.wikipedia.org/wiki/Science",
        "https://en.wikipedia.org:443/wiki/Science",
        "file:///wiki/Science",
        "https://en.wikipedia.org/wiki/",
        "https://en.wikipedia.org/wiki/Science%00",
        "https://en.wikipedia.org/wiki/%FF",
    ],
)
def test_non_article_or_malformed_urls_are_rejected(url):
    with pytest.raises((ValueError, UnicodeError)):
        wikipedia_topic(url)


def test_absent_topic_part_is_distinct_from_known_empty_and_ids_are_not_used():
    assert resource_topics({"kind": "youtube#video", "id": "v"}) is None
    assert resource_topics({"topicDetails": {}}) == []
    assert resource_topics({"topicDetails": {"topicIds": ["/m/science"]}}) == []
    result = resource_topics(
        {
            "topicDetails": {
                "topicCategories": [
                    "http://en.wikipedia.org/wiki/Science",
                    "https://en.wikipedia.org/wiki/Science",
                ]
            }
        }
    )
    assert len(result) == 1


@pytest.mark.parametrize(
    "details",
    [
        None,
        {"topicCategories": "Science"},
        {"topicCategories": [12]},
        {"topicCategories": ["x"] * 65},
    ],
)
def test_malformed_topic_part_fails_before_writes(details):
    with pytest.raises(ValueError):
        resource_topics({"topicDetails": details})


@pytest.mark.asyncio
@pytest.mark.parametrize("lookup", [lookup_videos, lookup_channels])
async def test_lookup_adds_topics_to_existing_batched_request_and_marks_empty(lookup):
    async def response(url, params, executor):
        return {"items": [{"id": key} for key in params["id"].split(",")]}

    with patch("atlas.youtube._execute_get", AsyncMock(side_effect=response)) as request:
        result = await lookup([str(i) for i in range(51)], executor=MagicMock())
    assert request.await_count == 2
    assert all("topicDetails" in c.args[1]["part"].split(",") for c in request.await_args_list)
    assert [len(c.args[1]["id"].split(",")) for c in request.await_args_list] == [50, 1]
    assert all(resource_topics(item) == [] for item in result)
    with patch("atlas.youtube._execute_get", AsyncMock(return_value={"items": [{"id": "v"}]})):
        partial = await lookup(["v"], parts="statistics", executor=MagicMock())
    assert resource_topics(partial[0]) is None


def test_graph_keeps_only_observed_facts_and_included_endpoints():
    url = "https://en.wikipedia.org/wiki/Science"
    nodes, edges = graph_projection(
        [{"url": url, "label": "Science", "language": "en"}],
        [{"id": "v", "title": "Example", "channel_id": "c", "status": "ARCHIVED"}],
        [],
        [
            {"video_id": "v", "topic_url": url, "observed_at": "now"},
            {"video_id": "missing", "topic_url": url, "observed_at": "now"},
        ],
        url,
    )
    assert len(nodes) == 3
    assert len(edges) == 2
    assert all(
        e["source"] in {n["id"] for n in nodes} and e["target"] in {n["id"] for n in nodes}
        for e in edges
    )
    # A publisher does not automatically inherit its video's topics.
    assert not any(e["source"] == "channel:c" and e["relation"] == "HAS_TOPIC" for e in edges)
    assert all("provenance" in e for e in edges)


@pytest.mark.asyncio
@pytest.mark.parametrize("kwargs", [{"limit": 51}, {"limit": 0}, {"search": "x" * 121}])
async def test_graph_bounds_reject_before_database_access(kwargs):
    rows = AsyncMock()
    with pytest.raises(ValueError):
        await KnowledgeGraphReader(rows).graph(**kwargs)
    rows.assert_not_called()
