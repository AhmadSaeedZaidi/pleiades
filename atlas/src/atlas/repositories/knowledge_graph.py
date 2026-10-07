"""A bounded graph projection over existing videos/channels and API topic facts."""

from collections.abc import Awaitable, Callable
from typing import Any

Rows = Callable[[str, tuple[Any, ...]], Awaitable[list[dict[str, Any]]]]


def graph_projection(
    topics: list[dict[str, Any]],
    videos: list[dict[str, Any]],
    direct_channels: list[dict[str, Any]],
    facts: list[dict[str, Any]],
    selected: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    nodes: dict[str, dict[str, Any]] = {}
    edges: dict[tuple[str, str, str], dict[str, Any]] = {}

    def add(kind: str, key: str, label: str, **attributes: Any) -> str:
        node_id = f"{kind}:{key}"
        nodes[node_id] = {"id": node_id, "kind": kind, "key": key, "label": label, **attributes}
        return node_id

    def edge(source: str, target: str, relation: str, **attributes: Any) -> None:
        edges[(source, target, relation)] = {
            "source": source,
            "target": target,
            "relation": relation,
            **attributes,
        }

    for topic in topics:
        add("topic", topic["url"], topic["label"], url=topic["url"], language=topic["language"])
    for video in videos:
        video_id = add(
            "video",
            video["id"],
            video["title"],
            status=video["status"],
            url=f"https://www.youtube.com/watch?v={video['id']}",
        )
        if video.get("channel_id"):
            channel_id = add(
                "channel",
                video["channel_id"],
                video.get("channel") or video["channel_id"],
                url=f"https://www.youtube.com/channel/{video['channel_id']}",
            )
            edge(video_id, channel_id, "PUBLISHED_BY", provenance="youtube.snippet.channelId")
    for channel in direct_channels:
        channel_id = add(
            "channel",
            channel["id"],
            channel["title"],
            url=f"https://www.youtube.com/channel/{channel['id']}",
        )
        edge(
            channel_id,
            f"topic:{selected}",
            "HAS_TOPIC",
            provenance="youtube.topicDetails.topicCategories",
            observed_at=channel["observed_at"],
        )
    for fact in facts:
        edge(
            f"video:{fact['video_id']}",
            f"topic:{fact['topic_url']}",
            "HAS_TOPIC",
            provenance="youtube.topicDetails.topicCategories",
            observed_at=fact["observed_at"],
        )
    # Every exported edge must connect included nodes, even with sparse metadata.
    valid = [edge for edge in edges.values() if edge["source"] in nodes and edge["target"] in nodes]
    return list(nodes.values()), valid


class KnowledgeGraphReader:
    def __init__(self, rows: Rows) -> None:
        self.rows = rows

    async def graph(self, search: str = "", topic: str = "", limit: int = 25) -> dict[str, Any]:
        if len(search) > 120 or not 1 <= limit <= 50:
            raise ValueError("Invalid graph bounds")
        summary = (
            await self.rows(
                """SELECT
            (SELECT count(*) FROM videos) AS video_total,
            (SELECT count(*) FROM channels) AS channel_total,
            (SELECT count(*) FROM knowledge_topics) AS topic_count,
            (SELECT count(*) FROM video_topics) AS video_edges,
            (SELECT count(*) FROM channel_topics) AS channel_edges,
            count(*) FILTER(WHERE kind='video' AND observed_at IS NOT NULL) AS video_checked,
            count(*) FILTER(WHERE kind='channel' AND observed_at IS NOT NULL) AS channel_checked,
            count(*) FILTER(WHERE outcome='empty') AS empty,
            count(*) FILTER(WHERE outcome='unavailable') AS unavailable,
            max(observed_at) AS latest_observed
            FROM youtube_topic_sync""",
                (),
            )
        )[0]
        term = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        catalog = await self.rows(
            """WITH catalog AS (SELECT t.*,
            (SELECT count(*) FROM video_topics v WHERE v.topic_url=t.url) AS videos,
            (SELECT count(*) FROM channel_topics c WHERE c.topic_url=t.url) AS channels
            FROM knowledge_topics t WHERE t.label ILIKE %s)
            SELECT * FROM catalog
            ORDER BY videos+channels DESC,label,url LIMIT 50""",
            (f"%{term}%",),
        )
        selected = topic or (catalog[0]["url"] if catalog else "")
        topics = (
            await self.rows("SELECT * FROM knowledge_topics WHERE url=%s", (selected,))
            if selected
            else []
        )
        videos: list[dict[str, Any]] = []
        channels: list[dict[str, Any]] = []
        facts: list[dict[str, Any]] = []
        if topics:
            videos = await self.rows(
                """SELECT v.id,v.title,v.channel_id,v.status,c.title AS channel
                FROM video_topics t JOIN videos v ON v.id=t.video_id
                LEFT JOIN channels c ON c.id=v.channel_id
                WHERE t.topic_url=%s ORDER BY v.discovered_at DESC,v.id LIMIT %s""",
                (selected, limit),
            )
            channels = await self.rows(
                """SELECT c.id,c.title,t.observed_at
                FROM channel_topics t JOIN channels c ON c.id=t.channel_id
                WHERE t.topic_url=%s ORDER BY c.id LIMIT 20""",
                (selected,),
            )
            ids = [v["id"] for v in videos]
            if ids:
                related = await self.rows(
                    """SELECT t.*,count(*) AS shared_sample_videos
                    FROM video_topics v JOIN knowledge_topics t ON t.url=v.topic_url
                    WHERE v.video_id=ANY(%s) AND t.url<>%s GROUP BY t.url
                    ORDER BY shared_sample_videos DESC,t.url LIMIT 12""",
                    (ids, selected),
                )
                topics += related
                facts = await self.rows(
                    """SELECT video_id,topic_url,observed_at FROM video_topics
                    WHERE video_id=ANY(%s) AND topic_url=ANY(%s)
                    ORDER BY video_id,topic_url""",
                    (ids, [t["url"] for t in topics]),
                )
        nodes, edges = graph_projection(topics, videos, channels, facts, selected)
        return {
            "summary": summary,
            "topics": catalog,
            "selected": selected if topics else None,
            "nodes": nodes,
            "edges": edges,
            "video_limit": limit,
            "sampled": True,
            "schema": "pleiades.topic-graph.v1",
        }
