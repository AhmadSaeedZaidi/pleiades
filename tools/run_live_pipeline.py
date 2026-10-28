"""Run the full Pleiades pipeline against REAL infrastructure on a single video.

Targets: a known-good public YouTube video (defaults to the Blender 4.0 tutorial
``B0J27sf9N1Y``). Writes to whichever vault is configured via ``HF_DATASET_ID``
(defaults to the production vault ``Rolaficus/pleiades-vault`` per ``.env``).

End-to-end steps:

    1. Resolve the target video via YouTube ``videos.list``.
    2. Resolve the channel via ``channels.list`` and snapshot it (channels +
       channel_stats_log).
    3. Persist the video metadata via ``VideoRepository.ingest_video_metadata``.
    4. Reset ``has_visuals`` / ``has_transcript`` flags so the agents pick it up.
    5. Run the Painter cycle  → frames in vault under ``frames/<video_id>/``.
    6. Run the Scribe cycle   → transcript in vault under ``transcripts/<video_id>.json``.
    7. Run the Tracker cycle  → row(s) in ``video_stats_log``.
    8. Print a final report and exit non-zero if any artifact is missing.

Usage:

    python tools/run_live_pipeline.py
    python tools/run_live_pipeline.py --video-id <YT_ID>
    python tools/run_live_pipeline.py --video-id <YT_ID> --no-tracker

Requires the same env vars as the live agents: ``DATABASE_URL``, ``HF_TOKEN``,
``HF_DATASET_ID``, ``YOUTUBE_API_KEY_POOL_JSON`` and (for cookies-aware
extraction) ``YOUTUBE_COOKIES_PATH``.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("pleiades.live_pipeline")


def _mirror_settings_to_environ() -> None:
    """Mirror ``atlas.config.settings`` into ``os.environ``.

    Several libraries (Prefect, huggingface_hub, ...) read ``os.environ`` directly
    and do not respect Pydantic's ``BaseSettings`` ``.env`` loading. Without this
    bridge, a clean shell (no Prefect env vars exported) will trigger Prefect's
    ephemeral SQLite DB start-up — which then fails alembic migrations.
    """
    from atlas.config import settings

    pairs: List[Tuple[str, str]] = []
    if settings.DATABASE_URL and not os.getenv("DATABASE_URL"):
        pairs.append(("DATABASE_URL", str(settings.DATABASE_URL)))
    if settings.HF_TOKEN and not os.getenv("HF_TOKEN"):
        pairs.append(("HF_TOKEN", settings.HF_TOKEN.get_secret_value()))
    if settings.HF_DATASET_ID and not os.getenv("HF_DATASET_ID"):
        pairs.append(("HF_DATASET_ID", settings.HF_DATASET_ID))
    if settings.VAULT_PROVIDER and not os.getenv("VAULT_PROVIDER"):
        pairs.append(("VAULT_PROVIDER", settings.VAULT_PROVIDER))
    if not os.getenv("YOUTUBE_API_KEY_POOL_JSON"):
        try:
            pairs.append(
                (
                    "YOUTUBE_API_KEY_POOL_JSON",
                    settings.YOUTUBE_API_KEY_POOL_JSON.get_secret_value(),
                )
            )
        except Exception:
            pass
    if settings.youtube_cookies_resolved_path and not os.getenv("YOUTUBE_COOKIES_PATH"):
        pairs.append(("YOUTUBE_COOKIES_PATH", settings.youtube_cookies_resolved_path))
    if settings.PREFECT_API_URL and not os.getenv("PREFECT_API_URL"):
        pairs.append(("PREFECT_API_URL", settings.PREFECT_API_URL))
    if settings.PREFECT_API_KEY and not os.getenv("PREFECT_API_KEY"):
        pairs.append(("PREFECT_API_KEY", settings.PREFECT_API_KEY.get_secret_value()))
    for k, v in pairs:
        os.environ[k] = v


def _build_video_data_for_ingest(item: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a ``videos.list`` API item to ``ingest_video_metadata``-compatible shape.

    The repository only reads from ``snippet`` plus a top-level ``id``. ``ingest`` already
    handles both ``id="VIDEOID"`` and ``id={"videoId": "VIDEOID"}`` shapes.
    """
    return {
        "id": item.get("id"),
        "snippet": item.get("snippet", {}),
    }


async def _resolve_targets(
    video_id: str,
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    """Resolve video + channel records via the YouTube Data API."""
    from atlas.youtube import lookup_channels, lookup_videos

    videos = await lookup_videos([video_id])
    if not videos:
        raise RuntimeError(
            f"YouTube videos.list returned 0 items for {video_id} — "
            "video may be deleted, private, or region-blocked."
        )
    video = videos[0]

    channel_id = video.get("snippet", {}).get("channelId")
    channel: Optional[Dict[str, Any]] = None
    if channel_id:
        channels = await lookup_channels([channel_id])
        if channels:
            channel = channels[0]
        else:
            logger.warning(f"channels.list returned 0 items for {channel_id}")
    else:
        logger.warning(f"video {video_id} has no channelId in snippet")
    return video, channel


async def _seed_database(
    video: Dict[str, Any], channel: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """Insert the channel snapshot + video metadata, reset processing flags."""
    from atlas.repositories import ChannelRepository, VideoRepository

    channel_repo = ChannelRepository()
    video_repo = VideoRepository()

    if channel is not None:
        await channel_repo.ingest_channel_snapshot(channel)
    await video_repo.ingest_video_metadata(_build_video_data_for_ingest(video))
    vid_id = video["id"]

    async with video_repo._connection() as conn:
        await conn.execute(
            """
            UPDATE videos
            SET has_visuals = FALSE,
                has_transcript = FALSE,
                status = 'PENDING',
                last_updated_at = NULL
            WHERE id = %s
            """,
            (vid_id,),
        )
        await conn.commit()

    row = await video_repo.get_by_id(vid_id)
    return row.model_dump() if row else {}


async def _run_painter() -> None:
    from maia.painter.flow import run_painter_cycle

    await run_painter_cycle(batch_size=1)


async def _run_scribe() -> None:
    from maia.scribe.flow import run_scribe_cycle

    await run_scribe_cycle(batch_size=1)


async def _run_tracker(video_id: str) -> None:
    """Log video statistics for this run's target (same code path as Tracker agent).

    We do **not** use :func:`tracker_flow` here: ``fetch_tracker_targets`` applies a
    global ``LIMIT`` across all stale videos. On a busy Neon database the Blender
    tutorial row may not appear in the first 50 candidates, so metrics would be
    skipped. Instead we call :func:`update_stats_task`` for exactly ``video_id``.
    """
    from atlas.repositories import VideoRepository
    from maia.strategies import YouTubeSearchStrategy
    from maia.tracker.flow import update_stats_task

    video_repo = VideoRepository()
    video = await video_repo.get_by_id(video_id)
    if not video:
        logger.warning("Tracker step: no video row for %s", video_id)
        return

    strategy = YouTubeSearchStrategy("tracking", agent_name="live_pipeline_tracker")
    n = await update_stats_task(
        [
            {
                "id": video_id,
                "title": video.title,
                "published_at": video.published_at,
                "last_updated_at": video.last_updated_at,
            }
        ],
        strategy,
    )
    logger.info("Tracker step: update_stats_task wrote %s video(s)", n)


async def _verify(video_id: str, channel_id: Optional[str]) -> List[Tuple[str, bool, str]]:
    """Return a list of ``(check_name, ok, detail)`` tuples for all artifacts."""
    from atlas.config import settings
    from atlas.repositories import ChannelRepository, VideoRepository
    from atlas.vault import get_vault

    video_repo = VideoRepository()
    channel_repo = ChannelRepository()
    results: List[Tuple[str, bool, str]] = []

    video = await video_repo.get_by_id(video_id)
    if video:
        results.append(
            (
                "videos row exists",
                True,
                f"title={video.title!r} status={video.status}",
            )
        )
        results.append(("videos.has_transcript", bool(video.has_transcript), ""))
        results.append(("videos.has_visuals", bool(video.has_visuals), ""))
    else:
        results.append(("videos row exists", False, "row missing"))

    if channel_id:
        ch = await channel_repo.get_by_id(channel_id)
        if ch:
            results.append(("channels row exists", True, f"title={ch.title!r}"))
        else:
            results.append(("channels row exists", False, "row missing"))

        ch_stats = await channel_repo.get_latest_stats(channel_id)
        if ch_stats:
            results.append(
                (
                    "channel_stats_log row exists",
                    True,
                    (
                        f"subs={ch_stats.subscriber_count} "
                        f"views={ch_stats.view_count} "
                        f"videos={ch_stats.video_count}"
                    ),
                )
            )
        else:
            results.append(("channel_stats_log row exists", False, "no stats logged"))

    vstats = await video_repo.get_latest_stats(video_id)
    if vstats:
        results.append(
            (
                "video_stats_log row exists",
                True,
                (f"views={vstats.views} likes={vstats.likes} comments={vstats.comment_count}"),
            )
        )
    else:
        results.append(("video_stats_log row exists", False, "no stats logged"))

    v = get_vault()
    frames = v.list_files(f"frames/{video_id}/")
    results.append(
        (
            f"vault frames/{video_id}/ has files",
            len(frames) > 0,
            f"{len(frames)} files (provider={settings.VAULT_PROVIDER}, dataset={settings.HF_DATASET_ID})",
        )
    )

    from atlas.vault import transcript_path

    transcripts = (
        v.list_files(transcript_path(video_id).removesuffix(".json") + "/")
        or v.list_files(transcript_path(video_id))
        or v.list_files(f"transcripts/{video_id}.json")
    )
    results.append(
        (
            f"vault transcript {video_id} exists",
            len(transcripts) > 0,
            f"{len(transcripts)} matches",
        )
    )

    return results


def _print_report(results: List[Tuple[str, bool, str]]) -> bool:
    print("\n" + "=" * 72)
    print("LIVE PIPELINE VERIFICATION REPORT")
    print("=" * 72)
    all_ok = True
    for name, ok, detail in results:
        marker = "OK " if ok else "FAIL"
        line = f"[{marker}] {name}"
        if detail:
            line += f"  ({detail})"
        print(line)
        all_ok = all_ok and ok
    print("=" * 72)
    print("RESULT:", "ALL CHECKS PASSED" if all_ok else "FAILURES DETECTED")
    print("=" * 72 + "\n")
    return all_ok


async def main_async(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    _mirror_settings_to_environ()
    from atlas.config import settings

    print(
        f"[live-pipeline] vault={settings.VAULT_PROVIDER} "
        f"dataset={settings.HF_DATASET_ID} video_id={args.video_id}"
    )

    print("[live-pipeline] resolving target video + channel via YouTube Data API…")
    video, channel = await _resolve_targets(args.video_id)
    channel_id = video.get("snippet", {}).get("channelId")
    print(
        f"[live-pipeline] resolved video '{video.get('snippet', {}).get('title')}' "
        f"channel='{(channel or {}).get('snippet', {}).get('title')}' (id={channel_id})"
    )

    print("[live-pipeline] seeding database…")
    seeded = await _seed_database(video, channel)
    print(
        f"[live-pipeline] seeded video row id={seeded.get('id')} "
        f"channel_id={seeded.get('channel_id')} status={seeded.get('status')}"
    )

    if not args.skip_painter:
        print("[live-pipeline] running Painter cycle…")
        await _run_painter()
    if not args.skip_scribe:
        print("[live-pipeline] running Scribe cycle…")
        await _run_scribe()
    if not args.skip_tracker:
        print("[live-pipeline] running Tracker cycle…")
        await _run_tracker(args.video_id)

    print("[live-pipeline] verifying artifacts…")
    results = await _verify(args.video_id, channel_id)
    ok = _print_report(results)
    return 0 if ok else 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--video-id",
        default="B0J27sf9N1Y",
        help="YouTube video ID to ingest (default: Blender 4.0 tutorial).",
    )
    parser.add_argument("--skip-painter", action="store_true")
    parser.add_argument("--skip-scribe", action="store_true")
    parser.add_argument("--skip-tracker", action="store_true")
    args = parser.parse_args()

    rc = asyncio.run(main_async(args))
    sys.exit(rc)


if __name__ == "__main__":
    main()
