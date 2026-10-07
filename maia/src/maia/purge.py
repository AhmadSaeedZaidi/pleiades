"""Purge short / low-quality videos from the database and the vault.

Removes videos shorter than a duration threshold (default 3 min) and any
artifacts they produced (audio, frames, video, transcript, metadata).
"""

import logging
from typing import Any

from atlas.repositories import VideoRepository
from atlas.vault import audio_path, get_vault, transcript_path, video_path

logger = logging.getLogger(__name__)

METADATA_PREFIX = "metadata/"
TRANSCRIPTS_PREFIX = "transcripts/"
FRAMES_PREFIX = "frames/"
AUDIO_PREFIX = "audio/"
VIDEO_PREFIX = "videos/"


def collect_artifact_paths(
    vault: Any,
    video_ids: list[str],
    metadata_files: list[str] | None = None,
) -> list[str]:
    """Return every existing vault path associated with the given video IDs.

    Paths are checked per prefix via ``list_files`` rather than probed
    individually, so the delete never 404s on artifacts that were never stored.
    """
    paths: list[str] = []

    existing_audio = set(vault.list_files(AUDIO_PREFIX)) | set(vault.list_files("media/audio/"))
    existing_video = set(vault.list_files(VIDEO_PREFIX))
    existing_transcripts = set(vault.list_files(TRANSCRIPTS_PREFIX))
    for vid in video_ids:
        for a in (audio_path(vid), f"audio/{vid}.opus"):
            if a in existing_audio:
                paths.append(a)
        audio_prefixes = (audio_path(vid).removesuffix(".opus") + "/", f"audio/{vid}/")
        paths.extend(path for path in existing_audio if path.startswith(audio_prefixes))
        v = video_path(vid)
        if v in existing_video:
            paths.append(v)
        t = transcript_path(vid)
        if t in existing_transcripts:
            paths.append(t)
        legacy_t = f"{TRANSCRIPTS_PREFIX}{vid}.json"
        if legacy_t in existing_transcripts:
            paths.append(legacy_t)
        version_prefix = f"transcripts/{vid[:2]}/{vid}/"
        paths.extend(path for path in existing_transcripts if path.startswith(version_prefix))

    frame_prefixes = {f"{FRAMES_PREFIX}{vid}/" for vid in video_ids}
    if frame_prefixes:
        paths.extend(
            f
            for f in vault.list_files(FRAMES_PREFIX)
            if any(f.startswith(p) for p in frame_prefixes)
        )

    meta_suffixes = {f"{vid}.json" for vid in video_ids}
    if metadata_files is None:
        metadata_files = vault.list_files(METADATA_PREFIX)
    paths.extend(
        f
        for f in metadata_files
        if f.startswith(METADATA_PREFIX)
        and (
            f.split("/")[-1] in meta_suffixes
            or (len(f.split("/")) == 4 and f.split("/")[2] in video_ids)
        )
    )

    return paths


async def purge_short_videos(
    min_duration: int = 180,
    dry_run: bool = True,
    delete_artifacts: bool = True,
) -> dict[str, Any]:
    """Remove videos shorter than ``min_duration`` seconds.

    Args:
        min_duration: Drop videos with ``duration < min_duration``.
        dry_run: When True (default), only report what *would* be removed.
        delete_artifacts: When True, also delete vault artifacts for those IDs.

    Returns a summary dict (counts + the affected IDs).
    """
    repo = VideoRepository()
    ids = await repo.find_short_video_ids(min_duration)

    result: dict[str, Any] = {
        "min_duration": min_duration,
        "dry_run": dry_run,
        "video_count": len(ids),
    }
    if not ids:
        logger.info("Purge: nothing to remove (no videos shorter than %ss).", min_duration)
        return result

    result["ids"] = ids

    if dry_run:
        logger.info(
            "Purge DRY-RUN: would remove %d videos shorter than %ss.",
            len(ids),
            min_duration,
        )
        return result

    vault = get_vault()
    if delete_artifacts:
        metadata_files = vault.list_files(METADATA_PREFIX)
        paths = collect_artifact_paths(vault, ids, metadata_files)
        artifacts_deleted = vault.delete_files(paths) if paths else 0
        result["artifacts_deleted"] = artifacts_deleted
        logger.info("Purge: deleted %d vault artifact files.", artifacts_deleted)

    removed = await repo.delete_videos(ids)
    result["videos_deleted"] = removed
    logger.info("Purge: deleted %d video rows from the database.", removed)
    return result


async def purge_transcripts(
    scope: str = "without_visuals",
    dry_run: bool = True,
) -> dict[str, Any]:
    """Reset (uncheck) transcripts for a scoped set of videos.

    A non-destructive "uncheck + organic overwrite" reset: videos go back to
    ``PENDING`` with ``has_transcript = FALSE`` but their transcript rows stay
    in place, so the Scribe re-upserts over them (``ON CONFLICT DO UPDATE``)
    instead of a delete-then-insert.

    Scopes: ``all``, ``without_visuals`` (default), ``without_audio``,
    ``pending``.

    Args:
        scope: Which transcripts to reset (see above).
        dry_run: When True (default), only report what *would* be reset.

    Returns a summary dict (scope, counts, affected IDs).
    """
    repo = VideoRepository()
    ids = await repo.find_transcript_video_ids(scope)

    result: dict[str, Any] = {
        "scope": scope,
        "dry_run": dry_run,
        "transcript_count": len(ids),
    }
    if not ids:
        logger.info("Transcript purge: nothing to reset for scope=%s.", scope)
        return result

    result["ids"] = ids

    if dry_run:
        logger.info(
            "Transcript purge DRY-RUN: would reset %d transcripts (scope=%s).",
            len(ids),
            scope,
        )
        return result

    reset = await repo.unmark_transcripts_batch(ids)
    result["transcripts_reset"] = reset
    logger.info("Transcript purge: reset %d transcripts (scope=%s).", reset, scope)
    return result
