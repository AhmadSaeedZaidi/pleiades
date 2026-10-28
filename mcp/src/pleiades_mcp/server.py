"""Pleiades MCP server.

Exposes Pleiades' YouTube intelligence capabilities to LLM clients over the
Model Context Protocol (FastMCP / stdio by default, with an optional HTTP+SSE
transport). Tools are self-contained and on-demand: a client supplies a YouTube
video id (or search query) and gets back search results, a transcript, keyframe
images, a cover thumbnail, an audio file, and a Mistral-generated transcript
summary — each as a downloadable artifact (``file://`` URI, or ``http://`` when
served over HTTP). Image tools can also return the picture inline so a
vision-capable model sees it directly.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import mimetypes
import tempfile
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP, Image

from pleiades_mcp.artifacts import ArtifactStore
from pleiades_mcp.media import (
    MediaUnavailableError,
    build_search_strategy,
    extract_audio_file,
    extract_keyframes,
    fetch_audio_segments,
    fetch_metadata,
    fetch_thumbnail,
    fetch_transcript_segments,
)
from pleiades_mcp.summarize import (
    SummarizationError,
    SummarizationUnavailableError,
)
from pleiades_mcp.summarize import (
    summarize_transcript as summarize_transcript_text,
)

logger = logging.getLogger("pleiades_mcp.server")

# Stable artifact cache root (override with PLEIADES_MCP_ARTIFACT_DIR).
_ARTIFACT_ROOT = Path(
    __import__("os").environ.get("PLEIADES_MCP_ARTIFACT_DIR", "/tmp/pleiades_mcp/artifacts")
)

_INSTRUCTIONS = """\
Pleiades MCP — on-demand YouTube intelligence for LLM agents.

Typical workflow:
  1. search_youtube(query)              -> discover videos, get video_id(s).
  2. get_video_metadata(video_id)       -> title, channel, duration, stats.
  3. summarize_transcript(video_id)     -> quick Markdown briefing (preferred).
     get_transcript(video_id)           -> raw captions if you need full text.
  4. get_keyframes(video_id)            -> representative frame images.
     get_thumbnail(video_id)            -> the video's cover image (1 image).
  5. get_audio(video_id, transcribe=T)  -> audio file; transcribe only when the
                                           video has NO captions.
  6. list_artifacts(video_id)           -> re-list downloadable URIs.

All tools accept either an 11-char video id (e.g. "dQw4w9WgXcQ") or a full
YouTube URL. Media results (summaries, transcripts, frames, audio) are saved as
artifacts and returned as downloadable http:// URIs. Prefer captions
(summarize_transcript / get_transcript) before paying for audio transcription.

Images: get_thumbnail returns the cover image inline by default; get_keyframes
returns download URLs only unless you pass inline=True (keep max_frames small
when inlining — each inline image costs many tokens).
"""

mcp = FastMCP("pleiades-mcp", instructions=_INSTRUCTIONS)
_store = ArtifactStore(_ARTIFACT_ROOT)
# Base URL used in artifact URIs when served over HTTP (set by main()).
_http_base: str | None = None


# ── helpers ──────────────────────────────────────────────────────────────────
def _artifact_uri(desc: dict[str, Any]) -> str:
    """Rewrite a file:// descriptor to an http:// URI when HTTP serving is on."""
    if _http_base is None:
        return desc["uri"]
    rel = desc["path"][len(str(_ARTIFACT_ROOT)) :].lstrip("/")
    return f"{_http_base.rstrip('/')}/{rel}"


def _video_id_from_url(value: str) -> str:
    """Best-effort extraction of an 11-char YouTube id from a url or raw id."""
    import re

    value = value.strip()
    m = re.search(r"(?:v=|youtu\.be/|/embed/|/shorts/)([\w-]{11})", value)
    if m:
        return m.group(1)
    if re.fullmatch(r"[\w-]{11}", value):
        return value
    raise ValueError(f"Could not parse a YouTube video id from: {value!r}")


# Cost ceilings.
#
# This server has no authentication, no per-client quota, and no rate limit, so
# the tool layer is the only place a single caller can be stopped from driving
# unbounded spend. `summarize_transcript_text` already truncates at 60k chars;
# these are the equivalent caps for the two expensive tools.
MAX_KEYFRAMES = 60
# Inline frames are base64 in the JSON-RPC response. 60 of them is tens of
# megabytes and a context-window blowout, so the ceiling is much lower than the
# URL-only path regardless of what the caller asks for.
MAX_INLINE_FRAMES = 6
# Paid transcription is billed per second of audio. Without a duration cap a
# single call on a multi-hour video is an unbounded invoice.
MAX_AUDIO_SECONDS = 3600


def _err(message: str) -> dict[str, Any]:
    return {"error": message}


def _duration_seconds(meta: dict[str, Any]) -> int | None:
    """Extract contentDetails.durationSeconds, or None if unavailable."""
    raw = (meta.get("contentDetails") or {}).get("durationSeconds")
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _artifact_dir(video_id: str | None) -> Path | None:
    """Resolve a (validated) artifact subdirectory, or the artifact root.

    Returns None if the id does not parse as a YouTube id, or if the resulting
    directory escapes the artifact root. Callers must treat None as "refuse".
    """
    root = _ARTIFACT_ROOT.resolve()
    if not video_id:
        return root
    try:
        safe_id = _video_id_from_url(video_id)
    except ValueError:
        return None
    candidate = (root / safe_id).resolve()
    # Defence in depth: _video_id_from_url already forbids '/' and '.' runs, but
    # the containment check is the property we actually depend on.
    if not candidate.is_relative_to(root):
        return None
    return candidate


# ── tools ─────────────────────────────────────────────────────────────────────
@mcp.tool()
async def search_youtube(
    query: str, max_results: int = 10, published_after_hours: int = 24
) -> dict[str, Any]:
    """Search YouTube for videos matching *query* and return candidate videos.

    Use this FIRST to discover videos; then pass a returned ``video_id`` (or the
    full YouTube URL) to the other tools (get_video_metadata, get_transcript,
    summarize_transcript, get_keyframes, get_audio).

    Args:
        query: search keywords, e.g. "cooking shorts viral".
        max_results: how many videos to return (1-50, default 10).
        published_after_hours: only videos published within this many hours
            (default 24). Raise for a wider window.

    Returns: {"query", "count", "results": [{"video_id", "title", "channel",
    "published_at", "description", "thumbnail", "watch_url"}, ...]}.
    Feed ``video_id`` from a result into the other tools.
    """
    from datetime import UTC, datetime, timedelta

    from maia.utils import video_id_of

    try:
        strategy = build_search_strategy(agent_name="mcp-search")
        params: dict[str, Any] = {
            "part": "snippet",
            "q": query,
            "type": "video",
            "maxResults": min(max(1, max_results), 50),
            "order": "date",
            "publishedAfter": (
                datetime.now(UTC) - timedelta(hours=published_after_hours)
            ).isoformat(),
        }
        resp = await strategy.search(params)
    except Exception as e:  # noqa: BLE001
        return _err(f"YouTube search failed: {e}")

    items = (resp or {}).get("items", [])
    results = []
    for item in items:
        snippet = item.get("snippet", {}) or {}
        vid = video_id_of(item)
        if not vid:
            continue
        results.append(
            {
                "video_id": vid,
                "title": snippet.get("title"),
                "channel": snippet.get("channelTitle"),
                "published_at": snippet.get("publishedAt"),
                "description": snippet.get("description"),
                "thumbnail": (
                    snippet.get("thumbnails", {}).get("medium", {}).get("url")
                    or snippet.get("thumbnails", {}).get("default", {}).get("url")
                ),
                "watch_url": f"https://www.youtube.com/watch?v={vid}",
            }
        )
    return {"query": query, "count": len(results), "results": results}


@mcp.tool()
async def get_video_metadata(video_id_or_url: str) -> dict[str, Any]:
    """Resolve structured metadata for a YouTube video via the YouTube Data API.

    Args:
        video_id_or_url: an 11-char YouTube video id (e.g. "dQw4w9WgXcQ") OR a
            full YouTube URL (watch?v=, youtu.be/, /shorts/, /embed/).

    Returns: {"video_id", "title", "channel_id", "channel_title",
    "published_at", "description", "tags", "duration" (ISO 8601 like "PT3M34S"),
    "statistics": {"views", "likes", "comments"}}.
    Use this to enrich results from search_youtube, or to check duration before
    fetching audio (long videos cost more to transcribe).
    """
    try:
        vid = _video_id_from_url(video_id_or_url)
    except ValueError as e:
        return _err(str(e))
    try:
        meta = await fetch_metadata(vid)
    except MediaUnavailableError as e:
        return _err(str(e))
    snippet = meta.get("snippet", {}) or {}
    content = meta.get("contentDetails", {}) or {}
    stats = meta.get("statistics", {}) or {}
    return {
        "video_id": vid,
        "title": snippet.get("title"),
        "channel_id": snippet.get("channelId"),
        "channel_title": snippet.get("channelTitle"),
        "published_at": snippet.get("publishedAt"),
        "description": snippet.get("description"),
        "tags": snippet.get("tags"),
        "duration": content.get("duration"),
        "statistics": {
            "views": stats.get("viewCount"),
            "likes": stats.get("likeCount"),
            "comments": stats.get("commentCount"),
        },
    }


@mcp.tool()
def get_transcript(video_id_or_url: str, format: str = "segments") -> dict[str, Any]:
    """Fetch a YouTube video's transcript/captions (free, no STT cost).

    Args:
        video_id_or_url: 11-char video id or full YouTube URL.
        format: one of "segments" (default), "text", "file".
            - "segments": list of {"text", "start", "duration"} objects.
            - "text": a single plain concatenated string.
            - "file": same as "text" but also saved to an artifact; the
              returned "artifact" URI can be downloaded directly.

    Returns: {"video_id", "segment_count", "format", and depending on format
    either "segments" or "text", or "artifact" + "content_type" + "size_bytes"}.
    For a short brief, prefer summarize_transcript instead.
    """
    try:
        vid = _video_id_from_url(video_id_or_url)
    except ValueError as e:
        return _err(str(e))
    try:
        segments = fetch_transcript_segments(vid)
    except MediaUnavailableError as e:
        return _err(str(e))

    text = "\n".join(
        (s.get("text") or "").strip() for s in segments if (s.get("text") or "").strip()
    )
    out: dict[str, Any] = {"video_id": vid, "segment_count": len(segments), "format": format}
    if format == "text":
        out["text"] = text
    elif format == "file":
        desc = _store.write_text(vid, "transcript", text, suffix="txt")
        out["artifact"] = _artifact_uri(desc)
        out["content_type"] = desc["content_type"]
        out["size_bytes"] = desc["size_bytes"]
    else:
        out["segments"] = segments
    return out


@mcp.tool()
def summarize_transcript(video_id_or_url: str, title: str | None = None) -> dict[str, Any]:
    """Fetch a video's transcript (captions) and summarize it with Mistral's
    chat model. Best for a quick briefing without reading the raw transcript.

    Args:
        video_id_or_url: 11-char video id or full YouTube URL.
        title: optional video title to give the model context (improves summary).

    Returns: {"video_id", "summary" (structured Markdown: overview, key points,
    entities, tone), "artifact" (downloadable .md URI), "content_type",
    "size_bytes"}. Requires a MISTRAL_API_KEY (configured on the server).
    """
    try:
        vid = _video_id_from_url(video_id_or_url)
    except ValueError as e:
        return _err(str(e))
    try:
        segments = fetch_transcript_segments(vid)
    except MediaUnavailableError as e:
        return _err(str(e))

    try:
        summary = summarize_transcript_text(segments, title=title)
    except SummarizationUnavailableError as e:
        return _err(str(e))
    except SummarizationError as e:
        return _err(str(e))

    desc = _store.write_text(vid, "summary", summary, suffix="md", content_type="text/markdown")
    return {
        "video_id": vid,
        "summary": summary,
        "artifact": _artifact_uri(desc),
        "content_type": desc["content_type"],
        "size_bytes": desc["size_bytes"],
    }


@mcp.tool()
def get_keyframes(video_id_or_url: str, max_frames: int = 12, inline: bool = False) -> Any:
    """Extract keyframe images from a YouTube video (ffmpeg surgical sampling).

    Frames are sampled on a uniform grid plus salient chapter/heatmap points.
    Each frame is saved as a webp artifact; the returned "uri" can be downloaded
    directly (http://...:8001/<video_id>/frames/...).

    Args:
        video_id_or_url: 11-char video id or full YouTube URL.
        max_frames: cap on number of frames returned (default 12, max 60).
        inline: if True, ALSO return the frames as inline image content so a
            vision-capable model can see them directly (no download needed).
            This costs many tokens per image — keep max_frames small (<=6) when
            inlining. Default False (URLs only).

    Returns: a JSON object {"video_id", "frame_count", "frames": [{"frame_index",
    "uri", "content_type", "size_bytes"}, ...]}. When inline=True the response
    also carries the frame images as inline image content blocks.
    """
    try:
        vid = _video_id_from_url(video_id_or_url)
    except ValueError as e:
        return _err(str(e))
    with tempfile.TemporaryDirectory() as tmp:
        try:
            frames = extract_keyframes(vid, Path(tmp))
        except MediaUnavailableError as e:
            return _err(str(e))
    frame_cap = MAX_INLINE_FRAMES if inline else MAX_KEYFRAMES
    frames = frames[: max(1, min(max_frames, frame_cap, len(frames)))]
    artifacts = []
    images: list[Image] = []
    for idx, data in frames:
        desc = _store.write_bytes(
            vid,
            "frames",
            data,
            suffix=f"{idx:06d}.webp",
            content_type="image/webp",
        )
        artifacts.append(
            {
                "frame_index": idx,
                "uri": _artifact_uri(desc),
                "content_type": desc["content_type"],
                "size_bytes": desc["size_bytes"],
            }
        )
        if inline:
            images.append(Image(data=data, format="webp"))
    summary = {"video_id": vid, "frame_count": len(artifacts), "frames": artifacts}
    if inline:
        return [summary, *images]
    return summary


@mcp.tool()
def get_thumbnail(video_id_or_url: str, inline: bool = True) -> Any:
    """Fetch a YouTube video's cover thumbnail (highest resolution available).

    Cheap single image — good for "what does this video look like" without
    extracting frames. The image is saved as an artifact and, by default, also
    returned as inline image content a vision model can see directly.

    Args:
        video_id_or_url: 11-char video id or full YouTube URL.
        inline: if True (default), also return the thumbnail as inline image
            content. Set False for just the download URL.

    Returns: a JSON object {"video_id", "artifact" (downloadable .jpg URL),
    "source_url", "content_type", "size_bytes"}. When inline=True the response
    also carries the thumbnail as an inline image content block.
    """
    try:
        vid = _video_id_from_url(video_id_or_url)
    except ValueError as e:
        return _err(str(e))
    try:
        data, source_url = fetch_thumbnail(vid)
    except MediaUnavailableError as e:
        return _err(str(e))
    desc = _store.write_bytes(vid, "thumbnail", data, suffix="jpg", content_type="image/jpeg")
    summary = {
        "video_id": vid,
        "artifact": _artifact_uri(desc),
        "source_url": source_url,
        "content_type": desc["content_type"],
        "size_bytes": desc["size_bytes"],
    }
    if inline:
        return [summary, Image(data=data, format="jpeg")]
    return summary


@mcp.tool()
async def get_audio(video_id_or_url: str, transcribe: bool = False) -> dict[str, Any]:
    """Download a YouTube video's speech-optimized audio track (opus).

    Use when there are NO captions (get_transcript/summarize_transcript failed)
    or when you specifically need the audio file. Transcription via Voxtral STT
    costs money and time, so only set transcribe=True when captions are absent.

    Args:
        video_id_or_url: 11-char video id or full YouTube URL.
        transcribe: if True, also run Mistral Voxtral STT and save a transcript
            artifact (default False). Refused for videos longer than
            MAX_AUDIO_SECONDS, because transcription is billed per second.

    Returns: {"video_id", "artifact" (downloadable .opus URI), "content_type",
    "size_bytes"}; if transcribe=True also "transcript_artifact" and
    "transcript_segment_count" (or "transcribe_error" on failure).
    """
    try:
        vid = _video_id_from_url(video_id_or_url)
    except ValueError as e:
        return _err(str(e))
    if transcribe:
        # Cost guard: refuse *before* spending on the download, using the cheap
        # metadata call. Without this a single call on a multi-hour video bills
        # an unbounded transcription.
        try:
            meta = await fetch_metadata(vid)
        except MediaUnavailableError as e:
            return _err(str(e))
        duration = _duration_seconds(meta)
        if duration and duration > MAX_AUDIO_SECONDS:
            return _err(
                f"Refusing to transcribe: video is {int(duration)}s "
                f"(> {MAX_AUDIO_SECONDS}s cap). Call get_audio(transcribe=False) to "
                "download the audio, then transcribe a slice yourself."
            )

    # extract_audio_file shells out to yt-dlp/ffmpeg and read_bytes pulls the whole
    # track into memory; neither belongs on the event loop, and the server has no
    # concurrency ceiling.
    def _download(dest: Path) -> bytes:
        return Path(extract_audio_file(vid, dest)).read_bytes()

    with tempfile.TemporaryDirectory() as tmp:
        try:
            data = await asyncio.to_thread(_download, Path(tmp))
        except MediaUnavailableError as e:
            return _err(str(e))
    desc = _store.write_bytes(vid, "audio", data, suffix="opus", content_type="audio/ogg")
    out = {
        "video_id": vid,
        "artifact": _artifact_uri(desc),
        "content_type": desc["content_type"],
        "size_bytes": desc["size_bytes"],
    }
    if transcribe:
        try:
            segments = await asyncio.to_thread(fetch_audio_segments, vid)
            text = "\n".join(
                (s.get("text") or "").strip() for s in segments if (s.get("text") or "").strip()
            )
            tdesc = _store.write_text(vid, "transcript_audio", text, suffix="txt")
            out["transcript_artifact"] = _artifact_uri(tdesc)
            out["transcript_segment_count"] = len(segments)
        except MediaUnavailableError as e:
            out["transcribe_error"] = str(e)
    return out


@mcp.tool()
def list_artifacts(video_id: str | None = None) -> dict[str, Any]:
    """List artifacts (summaries, transcripts, keyframes, audio) already cached
    by this server, with downloadable URIs.

    Args:
        video_id: optional 11-char id to filter to one video's artifacts. Omit
            to list everything cached this session.

    Returns: {"artifacts": [{"uri" (http download link), "path",
    "content_type", "size_bytes"}, ...]}. Use after get_keyframes / get_audio /
    summarize_transcript to retrieve the download links again.
    """
    root = _artifact_dir(video_id)
    if root is None:
        return _err(f"Not a valid YouTube video id: {video_id!r}")
    if not root.exists():
        return {"artifacts": []}
    base = _ARTIFACT_ROOT.resolve()
    out = []
    for p in sorted(root.rglob("*")):
        if not p.is_file():
            continue
        # Re-check containment per file: rglob can walk through directory
        # symlinks that point outside the artifact root.
        resolved = p.resolve()
        if not resolved.is_relative_to(base):
            logger.warning("Skipping artifact outside the store root: %s", resolved)
            continue
        rel = str(resolved.relative_to(base))
        uri = f"{_http_base}/{rel}" if _http_base else f"file://{resolved}"
        out.append(
            {
                "uri": uri,
                "path": str(resolved),
                "content_type": mimetypes.guess_type(p.name)[0] or "application/octet-stream",
                "size_bytes": resolved.stat().st_size,
            }
        )
    return {"artifacts": out}


# ── entrypoint ────────────────────────────────────────────────────────────────
def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pleiades-mcp", description="Pleiades MCP server")
    p.add_argument("--transport", choices=["stdio", "sse", "streamable-http"], default="stdio")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument(
        "--artifacts-port",
        type=int,
        default=None,
        help="Port for the static artifact file server (HTTP transport only).",
    )
    p.add_argument(
        "--allowed-host",
        action="append",
        default=None,
        metavar="HOST",
        help="Public host/IP clients connect through (e.g. 89.168.125.2). "
        "Repeatable. Added to the MCP DNS-rebinding-protection allowlist so "
        "requests with this Host header are not rejected with 421. When set, "
        "the corresponding http:// origin is allowed too.",
    )
    p.add_argument("--log-level", default="INFO")
    return p


def main() -> None:
    args = _build_arg_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    global _http_base
    if args.transport in ("sse", "streamable-http"):
        # The artifact static file server is launched separately (see
        # run_server_full.sh) on ART_PORT (default port+1). Point artifact URIs
        # at it so clients can download audio/keyframes/summaries over HTTP.
        art_port = args.artifacts_port or (args.port + 1)
        # Artifact URLs must use a host the *client* can reach. When bound to
        # 0.0.0.0, prefer the first --allowed-host (the public IP); never emit
        # 0.0.0.0 URLs (unreachable for remote clients).
        art_host = args.host
        if art_host in ("0.0.0.0", "::", ""):
            art_host = (args.allowed_host or ["127.0.0.1"])[0]
        _http_base = f"http://{art_host}:{art_port}"

        mcp.settings.host = args.host
        mcp.settings.port = args.port

        # DNS-rebinding protection defaults to allowing localhost only, so a
        # client connecting via the public IP is rejected with 421 "Invalid
        # Host header". Extend the allowlist with the public host(s) the client
        # actually connects through (keeps protection on; ingress is IP-locked).
        from mcp.server.transport_security import TransportSecuritySettings

        extra_hosts = list(args.allowed_host or [])
        if args.host not in ("127.0.0.1", "0.0.0.0", "localhost", "::1"):
            extra_hosts.append(args.host)
        allowed_hosts = ["127.0.0.1:*", "localhost:*", "[::1]:*"]
        allowed_origins = ["http://127.0.0.1:*", "http://localhost:*", "http://[::1]:*"]
        for h in extra_hosts:
            allowed_hosts += [h, f"{h}:*"]
            allowed_origins += [f"http://{h}", f"http://{h}:*"]
        mcp.settings.transport_security = TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=allowed_hosts,
            allowed_origins=allowed_origins,
        )

        logger.info(f"Pleiades MCP server ({args.transport}) on http://{args.host}:{args.port}")
        logger.info(f"Artifacts served at {_http_base}/")
        if extra_hosts:
            logger.info(f"Allowed Host header values: {extra_hosts}")
        mcp.run(transport=args.transport)
    else:
        logger.info("Pleiades MCP server (stdio) starting")
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
