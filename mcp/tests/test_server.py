"""Smoke tests for the Pleiades MCP server tools.

These exercise the tool wiring and artifact handling without hitting YouTube
or Mistral (network calls are monkeypatched). They verify that tools return the
expected structure and that artifacts are persisted and addressable.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

# Make the local packages importable.
ROOT = Path(__file__).resolve().parents[2]
for p in ("mcp/src", "atlas/src", "maia/src"):
    sys.path.insert(0, str(ROOT / p))

from maia import strategies as strategies_mod  # noqa: E402

import pleiades_mcp.server as server  # noqa: E402

VID = "dQw4w9WgXcQ"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("YOUTUBE_API_KEY_POOL_JSON", '["k1"]')
    monkeypatch.setenv("VAULT_PROVIDER", "huggingface")
    monkeypatch.setenv("HF_DATASET_ID", "mock/ds")
    monkeypatch.setenv("HF_TOKEN", "mock")


def test_search_youtube_parses_and_returns(monkeypatch):
    fake = {
        "items": [
            {
                "id": {"videoId": VID},
                "snippet": {
                    "title": "T",
                    "channelTitle": "C",
                    "publishedAt": "2026-01-01T00:00:00Z",
                },
            }
        ]
    }

    class _Strat:
        async def search(self, params):
            return fake

    monkeypatch.setattr(server, "build_search_strategy", lambda **k: _Strat())
    out = asyncio.run(server.search_youtube("cats", max_results=5))
    assert out["count"] == 1
    assert out["results"][0]["video_id"] == VID
    assert out["results"][0]["watch_url"].endswith(VID)


def test_build_search_strategy_uses_reserve_pool(monkeypatch):
    from pleiades_mcp import media

    monkeypatch.setenv("MCP_YOUTUBE_API_KEY_POOL_JSON", '["RES_A","RES_B"]')
    strat = media.build_search_strategy()
    assert strat.keys.pool_name == "mcp-reserve"
    assert strat.keys.keys == ["RES_A", "RES_B"]
    # rotates through both reserve keys, not the shared pool
    assert {strat.keys.next_key(), strat.keys.next_key()} == {"RES_A", "RES_B"}


def test_reserve_pool_supports_session_rotation_and_dead_keys(monkeypatch):
    from atlas.utils import QuotaExhaustedError

    from pleiades_mcp import media

    monkeypatch.setenv("MCP_YOUTUBE_API_KEY_POOL_JSON", '["RES_A","RES_B"]')
    ring = media.build_search_strategy().keys
    session_id = ring.start_session()

    assert ring.get_session_key(session_id) == "RES_A"
    ring.mark_key_dead("RES_A")
    assert ring.get_session_key(session_id) == "RES_B"
    assert ring.live_size == 1
    assert ring.attempt_rotation(session_id) is False
    with pytest.raises(QuotaExhaustedError):
        ring.get_session_key(session_id)

    ring.end_session(session_id)


def test_build_search_strategy_falls_back_without_reserve(monkeypatch):
    from pleiades_mcp import media

    monkeypatch.delenv("MCP_YOUTUBE_API_KEY_POOL_JSON", raising=False)
    # Neutralize the .env fallback so we exercise the true "no reserve" path.
    import dotenv

    monkeypatch.setattr(dotenv, "dotenv_values", lambda *a, **k: {})
    calls = {}

    class _Strat:
        def __init__(self, pool, agent_name=None):
            calls["pool"] = pool

    monkeypatch.setattr(strategies_mod, "YouTubeSearchStrategy", _Strat)
    media.build_search_strategy()
    assert calls["pool"] == "hunting"


def test_get_transcript_file_persists_artifact(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "_ARTIFACT_ROOT", tmp_path)
    server._store = server.ArtifactStore(tmp_path)
    monkeypatch.setattr(
        server,
        "fetch_transcript_segments",
        lambda vid: [{"text": "hello world", "start": 0.0, "duration": 1.0}],
    )
    out = server.get_transcript(VID, format="file")
    assert out["video_id"] == VID
    assert out["artifact"].startswith("file://")
    assert tmp_path.joinpath(VID, "transcript").exists()


def test_summarize_transcript_writes_artifact(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "_ARTIFACT_ROOT", tmp_path)
    server._store = server.ArtifactStore(tmp_path)
    monkeypatch.setattr(
        server,
        "fetch_transcript_segments",
        lambda vid: [{"text": "a b c", "start": 0.0, "duration": 1.0}],
    )
    monkeypatch.setattr(
        server, "summarize_transcript_text", lambda segs, **k: "# Summary\n- point one"
    )
    out = server.summarize_transcript(VID)
    assert "Summary" in out["summary"]
    assert out["artifact"].startswith("file://")
    assert tmp_path.joinpath(VID, "summary").exists()


def test_get_keyframes_persists_images(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "_ARTIFACT_ROOT", tmp_path)
    server._store = server.ArtifactStore(tmp_path)
    monkeypatch.setattr(
        server,
        "extract_keyframes",
        lambda vid, d: [(0, b"RIFFxxxxWEBPdata"), (30, b"RIFFxxxxWEBPdata")],
    )
    out = server.get_keyframes(VID, max_frames=2)
    assert out["frame_count"] == 2
    assert all(f["uri"].startswith("file://") for f in out["frames"])


def test_get_keyframes_inline_returns_images(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "_ARTIFACT_ROOT", tmp_path)
    server._store = server.ArtifactStore(tmp_path)
    monkeypatch.setattr(
        server,
        "extract_keyframes",
        lambda vid, d: [(0, b"RIFFxxxxWEBPdata"), (30, b"RIFFxxxxWEBPdata")],
    )
    out = server.get_keyframes(VID, max_frames=2, inline=True)
    assert isinstance(out, list)
    assert out[0]["frame_count"] == 2
    imgs = [x for x in out[1:] if isinstance(x, server.Image)]
    assert len(imgs) == 2


def test_get_thumbnail_inline_and_url(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "_ARTIFACT_ROOT", tmp_path)
    server._store = server.ArtifactStore(tmp_path)
    monkeypatch.setattr(
        server,
        "fetch_thumbnail",
        lambda vid: (b"\xff\xd8\xffjpegbytes", f"https://i.ytimg.com/vi/{vid}/maxresdefault.jpg"),
    )
    # inline (default): list with summary dict + one Image
    out = server.get_thumbnail(VID)
    assert isinstance(out, list)
    assert out[0]["video_id"] == VID
    assert out[0]["source_url"].endswith("maxresdefault.jpg")
    assert any(isinstance(x, server.Image) for x in out[1:])
    assert tmp_path.joinpath(VID, "thumbnail").exists()
    # URL-only
    out2 = server.get_thumbnail(VID, inline=False)
    assert isinstance(out2, dict)
    assert out2["artifact"].startswith("file://")


def test_get_thumbnail_unavailable_errors(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "_ARTIFACT_ROOT", tmp_path)
    server._store = server.ArtifactStore(tmp_path)

    def _boom(vid):
        raise server.MediaUnavailableError("no thumb")

    monkeypatch.setattr(server, "fetch_thumbnail", _boom)
    out = server.get_thumbnail(VID)
    assert "error" in out


def test_get_audio_persists_opus(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "_ARTIFACT_ROOT", tmp_path)
    server._store = server.ArtifactStore(tmp_path)
    fake = tmp_path / f"{VID}.opus"
    fake.write_bytes(b"fakeopus")
    monkeypatch.setattr(server, "extract_audio_file", lambda vid, d: fake)
    out = asyncio.run(server.get_audio(VID))
    assert out["video_id"] == VID
    assert out["artifact"].startswith("file://")


def test_get_audio_refuses_to_transcribe_an_over_long_video(monkeypatch, tmp_path):
    """Regression: transcription is billed per second with no cap.

    The server has no auth, no quota, and no rate limit, so before this a single
    call could download a multi-hour track and bill an unbounded STT invoice. The
    refusal happens before the download, using the cheap metadata call.
    """

    async def fake_meta(video_id):
        return {"contentDetails": {"durationSeconds": str(server.MAX_AUDIO_SECONDS + 60)}}

    monkeypatch.setattr(server, "fetch_metadata", fake_meta)
    monkeypatch.setattr(
        server, "extract_audio_file", lambda *a: pytest.fail("must refuse before downloading")
    )
    out = asyncio.run(server.get_audio(VID, transcribe=True))
    assert "error" in out
    assert "Refusing to transcribe" in out["error"]


def test_get_audio_allows_transcription_within_the_cap(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "_ARTIFACT_ROOT", tmp_path)
    server._store = server.ArtifactStore(tmp_path)
    fake = tmp_path / f"{VID}.opus"
    fake.write_bytes(b"fakeopus")
    monkeypatch.setattr(server, "extract_audio_file", lambda vid, d: fake)
    monkeypatch.setattr(
        server, "fetch_metadata", lambda vid: _async({"contentDetails": {"durationSeconds": "60"}})
    )
    monkeypatch.setattr(server, "fetch_audio_segments", lambda vid: [{"text": "hello"}])
    out = asyncio.run(server.get_audio(VID, transcribe=True))
    assert out["transcript_segment_count"] == 1
    assert "transcribe_error" not in out


def test_duration_seconds_tolerates_junk():
    assert server._duration_seconds({"contentDetails": {"durationSeconds": "90"}}) == 90
    assert server._duration_seconds({"contentDetails": {}}) is None
    assert server._duration_seconds({"contentDetails": {"durationSeconds": "nope"}}) is None
    assert server._duration_seconds({}) is None


def test_inline_keyframes_are_capped(monkeypatch, tmp_path):
    """Inline frames are base64 in the response; 60 of them is a context blowout."""
    monkeypatch.setattr(server, "_ARTIFACT_ROOT", tmp_path)
    server._store = server.ArtifactStore(tmp_path)
    frames = [(i, b"img") for i in range(60)]
    monkeypatch.setattr(server, "extract_keyframes", lambda vid, d: frames)
    out = server.get_keyframes(VID, max_frames=60, inline=True)
    assert len(out) == server.MAX_INLINE_FRAMES + 1  # +1 for the JSON summary block

    out_url = server.get_keyframes(VID, max_frames=60, inline=False)
    assert out_url["frame_count"] == server.MAX_KEYFRAMES


def test_artifact_store_evicts_to_its_budget(tmp_path):
    store = server.ArtifactStore(tmp_path, max_bytes=300)
    for i in range(10):
        store.write_bytes(VID, "audio", b"x" * 100, suffix=f"{i}.opus")
    total = sum(p.stat().st_size for p in tmp_path.rglob("*") if p.is_file())
    assert total <= 300, f"store exceeded its budget: {total} bytes"


def _async(value):
    async def _coro():
        return value

    return _coro()


def test_invalid_video_id_errors():
    out = server.get_transcript("not a video id !!")
    assert "error" in out


def test_list_artifacts_rejects_path_traversal(monkeypatch, tmp_path):
    """A traversal video_id must not enumerate files outside the store root.

    Regression: list_artifacts used to do `root / video_id` with no validation
    and then `p.relative_to(_ARTIFACT_ROOT)`, which is a *lexical* operation. A
    client could pass "../../.." and read back absolute paths and sizes for
    arbitrary files on the host (e.g. ../../../boot/System.map-*).
    """
    monkeypatch.setattr(server, "_ARTIFACT_ROOT", tmp_path)
    server._store = server.ArtifactStore(tmp_path)
    secret = tmp_path.parent / "secret.txt"
    secret.write_text("classified")

    for hostile in ("../../..", "../../secret.txt", "..", "/etc", "a/../../.."):
        out = server.list_artifacts(hostile)
        assert "error" in out, f"expected refusal for {hostile!r}, got {out}"
        # No artifact entries, and no resolved path or size from outside the root.
        assert "artifacts" not in out
        assert str(tmp_path.parent) not in str(out)
        assert str(secret) not in str(out)

    # Sanity: a real id resolves inside the root and lists nothing yet.
    assert server.list_artifacts(VID) == {"artifacts": []}


def test_list_artifacts_lists_only_cached_files(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "_ARTIFACT_ROOT", tmp_path)
    server._store = server.ArtifactStore(tmp_path)
    (tmp_path / VID / "transcript").mkdir(parents=True)
    (tmp_path / VID / "transcript" / "captions.txt").write_text("hello")

    out = server.list_artifacts(VID)
    assert len(out["artifacts"]) == 1
    entry = out["artifacts"][0]
    assert entry["path"].startswith(str(tmp_path.resolve()))
    assert entry["content_type"] == "text/plain"
    assert entry["size_bytes"] == 5


def test_list_artifacts_skips_symlink_escape(monkeypatch, tmp_path):
    """A symlink inside the store pointing outside it must not be listed."""
    monkeypatch.setattr(server, "_ARTIFACT_ROOT", tmp_path)
    server._store = server.ArtifactStore(tmp_path)
    outside = tmp_path.parent / "outside.txt"
    outside.write_text("nope")
    (tmp_path / VID).mkdir(parents=True, exist_ok=True)
    (tmp_path / VID / "leak.txt").symlink_to(outside)

    out = server.list_artifacts(VID)
    assert out["artifacts"] == []
