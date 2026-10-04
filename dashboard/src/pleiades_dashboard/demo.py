"""Deterministic example data, enabled only by an explicit --demo flag."""

from datetime import UTC, datetime, timedelta
from typing import Any

STAGES = ("raw", "audio", "visuals", "transcript", "clip")


class DemoRepository:
    def __init__(self) -> None:
        self.now = datetime.now(UTC)
        self.items: list[dict[str, Any]] = [
            {
                "id": f"demo{i:07d}",
                "title": title,
                "channel": channel,
                "duration": 480 + i * 76,
                "status": "PROCESSED" if i % 3 else "PROCESSING",
                "discovered_at": self.now - timedelta(minutes=i * 41),
                "published_at": self.now - timedelta(hours=i + 2),
                "views": 4820 + i * 1732,
                "tracking_tier": "HOURLY",
                **{f"{s}_phase": "DONE" if s != "clip" else "PENDING" for s in STAGES},
            }
            for i, (title, channel) in enumerate(
                [
                    ("Inside the next generation of space telescopes", "Science Journal"),
                    ("A quiet morning in the mountains", "Field Notes"),
                    ("How cities are redesigning public spaces", "Urban Stories"),
                    ("The science behind a perfect cup of coffee", "Everyday Science"),
                    ("Building a tiny home from the ground up", "Workshop"),
                    ("A closer look at the world's coral reefs", "Ocean Archive"),
                    ("Why this new battery design matters", "Engineering Explained"),
                    ("Walking through Kyoto in the rain", "Slow Travel"),
                ]
            )
        ]
        self.items[3]["visuals_phase"] = "FAILED"

    async def overview(self) -> dict[str, Any]:
        counts = {
            "total": 94308,
            "archived": 61204,
            "processed": 21837,
            "discovered": 146,
            "vault_pending": 24,
            "failed": 18,
        }
        stages = [
            {"name": s, "done": n, "failed": f}
            for s, n, f in zip(
                STAGES, [85302, 78941, 73160, 66230, 204], [3, 2, 8, 5, 0], strict=True
            )
        ]
        return {
            "counts": counts,
            "stages": stages,
            "tracking": {"total": 98469, "due": 204, "last_tracked_at": self.now},
            "timeline": [
                {
                    "hour": self.now.replace(minute=0, second=0, microsecond=0)
                    - timedelta(hours=23 - i),
                    "count": (i * 7 + 3) % 14,
                }
                for i in range(24)
            ],
        }

    async def videos(
        self,
        search: str = "",
        status: str = "",
        stage: str = "",
        page: int = 1,
        page_size: int = 25,
    ) -> dict[str, Any]:
        rows = [
            v
            for v in self.items
            if (not search or search.casefold() in (v["title"] + v["channel"] + v["id"]).casefold())
            and (not status or v["status"] == status)
            and (not stage or v[f"{stage}_phase"] == "FAILED")
        ]
        return {
            "items": rows[(page - 1) * page_size : page * page_size],
            "total": len(rows),
            "page": page,
            "page_size": page_size,
        }

    async def video(self, video_id: str) -> dict[str, Any] | None:
        item = next((v for v in self.items if v["id"] == video_id), None)
        if item is None:
            return None
        return {
            **item,
            "likes": 210,
            "comments": 43,
            "tags": ["documentary", "science"],
            "language": "en",
            "transcript_vaulted": False,
            "transcript_preview": '{"segments": [{"text": "This is sample transcript data."}]}',
            "transcript_truncated": False,
            "stats": [],
        }

    async def events(self) -> list[dict[str, Any]]:
        return [
            {
                "event_type": "janitor.cycle_complete",
                "entity_id": "janitor",
                "created_at": self.now - timedelta(minutes=i * 15),
            }
            for i in range(6)
        ]

    async def graph(self, search: str = "", topic: str = "", limit: int = 25) -> dict[str, Any]:
        # Demo mode deliberately has no Atlas imports or database credentials.
        catalog: list[dict[str, Any]] = [
            {
                "url": f"https://en.wikipedia.org/wiki/{key}",
                "label": label,
                "language": "en",
                "videos": 4,
                "channels": 2,
            }
            for key, label in [
                ("Science", "Science"),
                ("Lifestyle_(sociology)", "Lifestyle"),
                ("Technology", "Technology"),
                ("Tourism", "Tourism"),
            ]
        ]
        matching = [t for t in catalog if search.casefold() in t["label"].casefold()]
        selected = topic or (matching[0]["url"] if matching else "")
        chosen = next((t for t in catalog if t["url"] == selected), None)
        nodes: list[dict[str, Any]] = []
        edges: list[dict[str, Any]] = []
        if chosen:
            topics = [chosen, next(t for t in catalog if t != chosen)]
            nodes.extend(
                {
                    "id": f"topic:{t['url']}",
                    "kind": "topic",
                    "key": t["url"],
                    "label": t["label"],
                    "url": t["url"],
                    "language": "en",
                }
                for t in topics
            )
            for i, video in enumerate(self.items[:limit]):
                vid, channel = f"video:{video['id']}", f"channel:demo-channel-{i}"
                nodes.extend(
                    [
                        {
                            "id": vid,
                            "key": video["id"],
                            "kind": "video",
                            "label": video["title"],
                            "url": f"https://www.youtube.com/watch?v={video['id']}",
                        },
                        {
                            "id": channel,
                            "key": f"demo-channel-{i}",
                            "kind": "channel",
                            "label": video["channel"],
                            "url": "https://www.youtube.com/",
                        },
                    ]
                )
                edges.extend(
                    [
                        {
                            "source": vid,
                            "target": channel,
                            "relation": "PUBLISHED_BY",
                            "provenance": "youtube.snippet.channelId",
                        },
                        {
                            "source": vid,
                            "target": f"topic:{chosen['url']}",
                            "relation": "HAS_TOPIC",
                            "provenance": "youtube.topicDetails.topicCategories",
                            "observed_at": self.now,
                        },
                    ]
                )
                if i % 2 == 0:
                    edges.append(
                        {
                            "source": vid,
                            "target": f"topic:{topics[1]['url']}",
                            "relation": "HAS_TOPIC",
                            "provenance": "youtube.topicDetails.topicCategories",
                            "observed_at": self.now,
                        }
                    )
        return {
            "summary": {
                "topic_count": 4,
                "video_edges": 16,
                "channel_edges": 8,
                "video_checked": 8,
                "channel_checked": 8,
                "video_total": 8,
                "channel_total": 8,
                "empty": 0,
                "unavailable": 0,
                "latest_observed": self.now,
            },
            "topics": matching,
            "selected": selected if chosen else None,
            "nodes": nodes,
            "edges": edges,
            "video_limit": limit,
            "sampled": True,
            "schema": "pleiades.topic-graph.v1",
        }

    async def queries(self) -> list[dict[str, Any]]:
        return [
            {
                "query_term": term,
                "priority": 3,
                "mention_count": 8 + i,
                "status": "PENDING",
                "result_count_total": i * 40,
                "last_searched_at": self.now - timedelta(hours=i),
            }
            for i, term in enumerate(
                [
                    "space exploration",
                    "architecture",
                    "science",
                    "nature documentary",
                    "engineering",
                ]
            )
        ]
