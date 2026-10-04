"""YouTube's Wikipedia topic URLs, without fetching arbitrary remote pages."""

import re
import unicodedata
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, unquote, urlsplit


@dataclass(frozen=True)
class Topic:
    url: str
    label: str
    language: str


def wikipedia_topic(value: str) -> Topic:
    if not isinstance(value, str) or len(value) > 1024 or any(ord(c) < 32 for c in value):
        raise ValueError("Invalid Wikipedia topic URL")
    parsed = urlsplit(value)
    host = parsed.hostname or ""
    match = re.fullmatch(r"([a-z][a-z0-9-]{1,15})(?:\.m)?\.wikipedia\.org", host)
    if (
        parsed.scheme not in {"http", "https"}
        or not match
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or not parsed.path.startswith("/wiki/")
    ):
        raise ValueError("Expected a Wikipedia article URL")
    title = unicodedata.normalize("NFC", unquote(parsed.path[6:], errors="strict"))
    title = "_".join(title.replace("_", " ").split())
    if not title or len(title) > 300 or any(ord(c) < 32 for c in title):
        raise ValueError("Invalid Wikipedia article title")
    language = match[1]
    return Topic(
        url=f"https://{language}.wikipedia.org/wiki/{quote(title, safe='()/,:_-')}",
        label=title.replace("_", " "),
        language=language,
    )


def resource_topics(resource: dict[str, Any]) -> list[Topic] | None:
    """None means unobserved; [] means the full API resource has no topics.

    Search snippets and partial statistics responses must not erase known edges.
    Malformed topic payloads fail before any database mutation.
    """
    if "topicDetails" not in resource:
        return None
    details = resource["topicDetails"]
    if not isinstance(details, dict):
        raise ValueError("Invalid YouTube topicDetails")
    categories = details.get("topicCategories", [])
    if not isinstance(categories, list) or len(categories) > 64:
        raise ValueError("Invalid YouTube topicCategories")
    topics = [wikipedia_topic(url) for url in categories]
    return sorted({topic.url: topic for topic in topics}.values(), key=lambda topic: topic.url)
