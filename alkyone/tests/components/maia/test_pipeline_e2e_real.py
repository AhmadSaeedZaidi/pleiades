"""
END-TO-END pipeline integration test against REAL infrastructure.

This is the canonical test that proves the whole Maia pipeline works:

    YouTube Data API  ──▶  channels.list  ──▶  ingest_channel_snapshot
                                              (channels + channel_stats_log)
    YouTube Data API  ──▶  videos.list   ──▶  ingest_video_metadata
                                              (videos + channel skeleton)

    yt-dlp + cookies  ──▶  Painter cycle  ──▶  frames/<id>/*.jpg in vault
                                                     + has_visuals=TRUE
    youtube-transcript-api ──▶  Scribe cycle ──▶  transcripts/<id>.json in vault
                                                     + has_transcript=TRUE
    YouTube Data API  ──▶  Tracker cycle  ──▶  video_stats_log row

Asserts (real Neon DB + HuggingFace):

    1. videos row exists with rich metadata.
    2. channels row exists with title resolved from the API (not placeholder).
    3. channel_stats_log row exists with subscriber/view/video counts > 0.
    4. has_visuals = TRUE; vault has at least one ``frames/<id>/*.jpg`` (Painter uses *yt-dlp*).
    5. has_transcript = TRUE; vault has ``transcripts/<id>.json``.
    6. video_stats_log row exists with views > 0 (Tracker metrics).

By default, ``alkyone`` test fixtures set ``HF_DATASET_ID`` to
``Rolaficus/pleiades-vault-test`` so integration runs do not pollute production.
To run this test against the **production** dataset (``Rolaficus/pleiades-vault`` from
``.env``)::

    PLEIADES_USE_PRODUCTION_VAULT=1 pytest ... -k test_full_pipeline_real_blender

To verify the same end-to-end **without** pytest, use
``python tools/run_live_pipeline.py`` (uses ``HF_DATASET_ID`` from ``.env``).

Target video: ``B0J27sf9N1Y`` — Blender 4.0 tutorial (Blender Guru).
"""

from __future__ import annotations

import logging
import os
from typing import Any

import pytest
import pytest_asyncio

logger = logging.getLogger(__name__)


VIDEO_ID = "B0J27sf9N1Y"


@pytest_asyncio.fixture
async def video_repo(fresh_db):
    """Provide VideoRepository against the (already-cleaned) test DB."""
    from atlas.repositories import VideoRepository

    yield VideoRepository()


@pytest_asyncio.fixture
async def channel_repo(fresh_db):
    """Provide ChannelRepository against the (already-cleaned) test DB."""
    from atlas.repositories import ChannelRepository

    yield ChannelRepository()


async def _resolve_via_api(
    video_id: str,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Resolve video + channel metadata via YouTube Data API."""
    from atlas.youtube import lookup_channels, lookup_videos

    videos = await lookup_videos([video_id])
    if not videos:
        pytest.skip(f"YouTube videos.list returned 0 items for {video_id} (deleted/private?)")
    video = videos[0]

    channel_id = video.get("snippet", {}).get("channelId")
    channel: dict[str, Any] | None = None
    if channel_id:
        channels = await lookup_channels([channel_id])
        if channels:
            channel = channels[0]
    return video, channel


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.timeout(600)
async def test_full_pipeline_real_blender(video_repo, channel_repo):
    """Run the entire pipeline against real infrastructure on B0J27sf9N1Y.

    Each stage must succeed and produce verifiable artifacts in:
      - Neon Postgres (videos, channels, channel_stats_log, video_stats_log)
      - HuggingFace test vault (frames/, transcripts/)
    """
    print(f"\n[e2e] target video_id={VIDEO_ID}")

    print("[e2e] step 1/5 — resolving via YouTube Data API…")
    video_item, channel_item = await _resolve_via_api(VIDEO_ID)
    title = video_item.get("snippet", {}).get("title")
    channel_id = video_item.get("snippet", {}).get("channelId")
    assert title, f"video {VIDEO_ID} has no title in API response"
    assert channel_id, f"video {VIDEO_ID} has no channelId in API response"
    assert channel_item, f"channels.list returned 0 items for channel {channel_id}"
    channel_title = channel_item.get("snippet", {}).get("title")
    print(f"[e2e]   resolved video='{title}' channel='{channel_title}' (id={channel_id})")

    print("[e2e] step 2/5 — seeding DB (channel snapshot + video metadata)…")
    await channel_repo.ingest_channel_snapshot(channel_item)
    await video_repo.ingest_video_metadata(
        {"id": video_item.get("id"), "snippet": video_item.get("snippet", {})}
    )
    await video_repo._execute(
        """
        UPDATE videos
        SET has_visuals = FALSE,
            has_transcript = FALSE,
            status = 'PENDING',
            last_updated_at = NULL
        WHERE id = %s
        """,
        (VIDEO_ID,),
    )

    seeded_video = await video_repo.get_by_id(VIDEO_ID)
    seeded_channel = await channel_repo.get_by_id(channel_id)
    seeded_ch_stats = await channel_repo.get_latest_stats(channel_id)

    assert seeded_video is not None, "video row missing after ingest"
    assert seeded_channel is not None, "channel row missing after ingest_channel_snapshot"
    assert seeded_channel.title == channel_title, (
        f"channel title mismatch: db={seeded_channel.title!r} api={channel_title!r}"
    )
    assert seeded_ch_stats is not None, (
        "channel_stats_log row missing — ingest_channel_snapshot didn't log stats"
    )
    subs = seeded_ch_stats.subscriber_count
    views = seeded_ch_stats.view_count
    vcount = seeded_ch_stats.video_count
    assert subs is not None and subs > 0, f"channel subscriber_count not logged correctly: {subs}"
    assert views is not None and views > 0, f"channel view_count not logged correctly: {views}"
    assert vcount is not None and vcount > 0, f"channel video_count not logged correctly: {vcount}"
    print(f"[e2e]   channel_stats_log row OK — subs={subs} views={views} videos={vcount}")

    print("[e2e] step 3/5 — running Painter cycle (yt-dlp + ffmpeg)…")
    from maia.painter.flow import run_painter_cycle

    try:
        await run_painter_cycle(batch_size=1)
    except Exception as e:
        msg = str(e)
        if "Sign in" in msg or "bot" in msg.lower():
            pytest.skip(f"YouTube anti-bot blocked extraction: {e}")
        if "429" in msg:
            pytest.skip(f"YouTube rate limit (429) during Painter cycle: {e}")
        raise

    print("[e2e] step 4/5 — running Scribe cycle (transcript fetch)…")
    from maia.scribe.flow import run_scribe_cycle

    try:
        await run_scribe_cycle(batch_size=1)
    except Exception as e:
        if "429" in str(e):
            pytest.skip(f"YouTube transcript API rate-limited: {e}")
        raise

    print("[e2e] step 5/5 — running Tracker cycle (statistics)…")
    # Painter / Scribe both just bumped ``last_updated_at`` to NOW() — that makes
    # this row "too fresh" for ``fetch_tracker_targets`` (which filters on tier
    # staleness). Reset it so the Tracker tier-1/2/3 query picks the row up.
    await video_repo._execute(
        "UPDATE videos SET last_updated_at = NULL WHERE id = %s",
        (VIDEO_ID,),
    )

    from atlas.utils import KeyRing, ResiliencyExecutor
    from maia.tracker.flow import tracker_flow

    keys = KeyRing("tracking")
    executor = ResiliencyExecutor(keys, agent_name="tracker")
    await tracker_flow(batch_size=50, executor=executor)

    print("[e2e] verifying database state…")
    final_video = await video_repo.get_by_id(VIDEO_ID)
    assert final_video is not None
    assert final_video.status != "FAILED", f"video ended up FAILED: {final_video}"
    assert final_video.has_visuals is True, "Painter did not mark has_visuals=TRUE"
    assert final_video.has_transcript is True, "Scribe did not mark has_transcript=TRUE"

    vstats = await video_repo.get_latest_stats(VIDEO_ID)
    assert vstats is not None, "Tracker did not insert any video_stats_log row"
    assert vstats.views is not None and vstats.views > 0, (
        f"video_stats_log views not logged correctly: {vstats}"
    )
    print(
        f"[e2e]   video_stats_log row OK — views={vstats.views} "
        f"likes={vstats.likes} comments={vstats.comment_count}"
    )

    print("[e2e] verifying vault artifacts…")
    from atlas.config import settings
    from atlas.vault import get_vault, transcript_path
    from huggingface_hub import HfApi

    if settings.VAULT_PROVIDER == "huggingface":
        hf_token = os.getenv("HF_TOKEN")
        hf_dataset = os.getenv("HF_DATASET_ID")
        assert hf_token and hf_dataset, "HF_TOKEN/HF_DATASET_ID must be set to verify vault writes"

        api = HfApi(token=hf_token)
        files = api.list_repo_files(repo_id=hf_dataset, repo_type="dataset")
        frames = [f for f in files if f.startswith(f"frames/{VIDEO_ID}/")]
        transcripts = [
            f
            for f in files
            if f == f"transcripts/{VIDEO_ID}.json"
            or f.endswith(f"/{VIDEO_ID}.json")
            and f.startswith("transcripts/")
        ]

        assert len(frames) > 0, (
            f"No frames in vault {hf_dataset!r} for {VIDEO_ID}. Painter ran but did not upload."
        )
        assert len(transcripts) == 1, (
            f"Transcript not in vault {hf_dataset!r} for {VIDEO_ID}. Got: {transcripts}"
        )

        from alkyone.fixtures import track_hf_upload

        for f in frames + transcripts:
            track_hf_upload(f)

        print(f"[e2e]   vault frames OK — {len(frames)} files in {hf_dataset}")
        print(f"[e2e]   vault transcript OK — {transcripts[0]} in {hf_dataset}")
    else:
        v = get_vault()
        assert len(v.list_files(f"frames/{VIDEO_ID}/")) > 0
        sharded = v.list_files(transcript_path(VIDEO_ID))
        flat = v.list_files(f"transcripts/{VIDEO_ID}.json")
        versioned = v.list_files(transcript_path(VIDEO_ID).removesuffix(".json") + "/")
        assert sharded or flat or versioned
        assert v.fetch_transcript(VIDEO_ID) is not None

    print("[e2e] ALL CHECKS PASSED — full pipeline produced real artifacts on real infra.")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_channel_auto_discovery_from_search_item(video_repo, channel_repo):
    """When a video's channel is not yet indexed, ``enrich_channels_task`` populates it.

    This covers the user's requirement:
      "channel information needs to be tracked as well, any time a video
       is found and its channel is not indexed."

    We feed a single, real ``snippet.channelId`` (Blender Guru) to the enrichment
    task and verify both ``channels`` and ``channel_stats_log`` rows are created
    with realistic data fetched from the live YouTube API.
    """
    from atlas.utils import KeyRing, ResiliencyExecutor
    from maia.hunter.flow import enrich_channels_task

    blender_guru_id = "UCOKHwx1VCdgnxwbjyb9Iu1g"

    pre_channel = await channel_repo.get_by_id(blender_guru_id)
    pre_stats = await channel_repo.get_latest_stats(blender_guru_id)
    assert pre_channel is None and pre_stats is None, (
        "fresh_db should have wiped channel data — got pre-existing rows"
    )

    keys = KeyRing("hunting")
    executor = ResiliencyExecutor(keys, agent_name="hunter_enrich_test")

    written = await enrich_channels_task([blender_guru_id], executor)
    assert written == 1, f"Expected 1 channel snapshot to be written; got {written}"

    post_channel = await channel_repo.get_by_id(blender_guru_id)
    assert post_channel is not None, "channels row not created"
    assert post_channel.title, "channels.title is empty"
    assert post_channel.title != blender_guru_id, (
        "channels.title was not enriched with the real channel name"
    )

    post_stats = await channel_repo.get_latest_stats(blender_guru_id)
    assert post_stats is not None, "channel_stats_log row not created"
    assert (post_stats.subscriber_count or 0) > 0, "subscriber_count must be >0 from real API"
    assert (post_stats.view_count or 0) > 0
    assert (post_stats.video_count or 0) > 0

    again = await enrich_channels_task([blender_guru_id], executor)
    assert again == 0, (
        f"enrich_channels_task should be a no-op when stats are <24h old; got {again} re-fetches"
    )
