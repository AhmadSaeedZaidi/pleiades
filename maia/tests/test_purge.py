"""Tests for the short-video purge and the ingestion-quality report."""

import pytest
from maia.purge import collect_artifact_paths, purge_short_videos, purge_transcripts
from maia.quality_report import report_ingestion_quality


class _FakeVault:
    def __init__(self):
        self.deleted: list[str] = []
        self.audio_files = ["audio/ABC.opus", "audio/DEF.opus"]
        self.video_files = ["videos/ABC.mp4"]
        self.transcript_files = ["transcripts/ABC.json", "transcripts/DEF.json"]
        self.frame_files = [
            "frames/ABC/0.webp",
            "frames/DEF/0.webp",
            "frames/OTHER/0.webp",
        ]
        self.meta_files = [
            "metadata/2025-01-01/ABC.json",
            "metadata/2025-01-01/DEF.json",
            "metadata/2025-01-01/GHI.json",
        ]

    def list_files(self, prefix: str) -> list[str]:
        if prefix.startswith(("audio/", "media/audio/")):
            return [path for path in self.audio_files if path.startswith(prefix)]
        if prefix.startswith("videos/"):
            return list(self.video_files)
        if prefix.startswith("transcripts/"):
            return list(self.transcript_files)
        if prefix.startswith("frames/"):
            return [f for f in self.frame_files if f.startswith(prefix)]
        if prefix.startswith("metadata/"):
            return list(self.meta_files)
        return []

    def delete_files(self, paths: list[str]) -> int:
        self.deleted.extend(paths)
        return len(paths)


def test_collect_artifact_paths():
    vault = _FakeVault()
    paths = collect_artifact_paths(vault, ["ABC", "DEF"])
    assert "audio/ABC.opus" in paths
    assert "videos/ABC.mp4" in paths
    assert "transcripts/ABC.json" in paths
    assert "frames/ABC/0.webp" in paths
    assert "metadata/2025-01-01/ABC.json" in paths
    # Only the requested videos' artifacts.
    assert "frames/OTHER/0.webp" not in paths
    assert "metadata/2025-01-01/GHI.json" not in paths


def test_collect_audio_includes_legacy_and_sharded_chunks():
    vault = _FakeVault()
    vault.audio_files += [
        "media/audio/AB/ABC.opus",
        "media/audio/AB/ABC/000.opus",
        "audio/ABC/001.opus",
        "media/audio/AB/OTHER.opus",
    ]
    paths = collect_artifact_paths(vault, ["ABC"])
    assert set(path for path in paths if path.startswith(("audio/", "media/audio/"))) == {
        "audio/ABC.opus",
        "media/audio/AB/ABC.opus",
        "media/audio/AB/ABC/000.opus",
        "audio/ABC/001.opus",
    }


class _FakeRepo:
    def __init__(self):
        self.deleted_rows = 0
        self.ids: list[str] = []

    async def find_short_video_ids(self, max_duration: int, limit=None):
        return ["ABC", "DEF"]

    async def delete_videos(self, ids: list[str]) -> int:
        self.ids = ids
        self.deleted_rows = len(ids)
        return len(ids)


@pytest.mark.asyncio
async def test_purge_dry_run_deletes_nothing(monkeypatch):
    repo = _FakeRepo()
    vault = _FakeVault()
    monkeypatch.setattr("maia.purge.VideoRepository", lambda: repo)
    monkeypatch.setattr("maia.purge.get_vault", lambda: vault)

    result = await purge_short_videos(min_duration=180, dry_run=True)
    assert result["video_count"] == 2
    assert "videos_deleted" not in result
    assert vault.deleted == []
    assert repo.deleted_rows == 0


@pytest.mark.asyncio
async def test_purge_confirm_removes_videos_and_artifacts(monkeypatch):
    repo = _FakeRepo()
    vault = _FakeVault()
    monkeypatch.setattr("maia.purge.VideoRepository", lambda: repo)
    monkeypatch.setattr("maia.purge.get_vault", lambda: vault)

    result = await purge_short_videos(min_duration=180, dry_run=False, delete_artifacts=True)
    assert result["videos_deleted"] == 2
    assert vault.deleted  # artifact files removed
    assert repo.ids == ["ABC", "DEF"]


@pytest.mark.asyncio
async def test_purge_keep_artifacts(monkeypatch):
    repo = _FakeRepo()
    vault = _FakeVault()
    monkeypatch.setattr("maia.purge.VideoRepository", lambda: repo)
    monkeypatch.setattr("maia.purge.get_vault", lambda: vault)

    result = await purge_short_videos(min_duration=180, dry_run=False, delete_artifacts=False)
    assert result["videos_deleted"] == 2
    assert vault.deleted == []  # vault left untouched


@pytest.mark.asyncio
async def test_quality_report(monkeypatch):
    async def _report(self):
        return {"total_videos": 1, "shorts_under_3m": 0}

    monkeypatch.setattr(
        "atlas.repositories.video.quality.VideoQualityMixin.quality_report", _report
    )
    out = await report_ingestion_quality()
    assert out["total_videos"] == 1


class _FakeTranscriptRepo:
    def __init__(self, ids):
        self.ids = ids
        self.reset_ids: list[str] = []

    async def find_transcript_video_ids(self, scope="without_visuals"):
        self.scope = scope
        return self.ids

    async def unmark_transcripts_batch(self, video_ids):
        self.reset_ids = video_ids
        return len(video_ids)


@pytest.mark.asyncio
async def test_purge_transcripts_dry_run_resets_nothing(monkeypatch):
    repo = _FakeTranscriptRepo(["AAA", "BBB"])
    monkeypatch.setattr("maia.purge.VideoRepository", lambda: repo)

    result = await purge_transcripts(scope="without_visuals", dry_run=True)
    assert result["transcript_count"] == 2
    assert "transcripts_reset" not in result
    assert repo.reset_ids == []  # no DB write in dry-run


@pytest.mark.asyncio
async def test_purge_transcripts_confirm_unchecks(monkeypatch):
    repo = _FakeTranscriptRepo(["AAA", "BBB", "CCC"])
    monkeypatch.setattr("maia.purge.VideoRepository", lambda: repo)

    result = await purge_transcripts(scope="all", dry_run=False)
    assert result["transcripts_reset"] == 3
    assert repo.scope == "all"
    assert repo.reset_ids == ["AAA", "BBB", "CCC"]


@pytest.mark.asyncio
async def test_purge_transcripts_empty_scope(monkeypatch):
    repo = _FakeTranscriptRepo([])
    monkeypatch.setattr("maia.purge.VideoRepository", lambda: repo)

    result = await purge_transcripts(scope="pending", dry_run=False)
    assert result["transcript_count"] == 0
    assert "transcripts_reset" not in result
    assert repo.reset_ids == []
