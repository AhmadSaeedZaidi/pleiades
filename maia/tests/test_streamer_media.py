"""Regression tests for StealthVideoStreamer audio-file discovery.

These lock in the fix for the historical "raw_uri pointed at metadata /
partial files" bug: `_find_audio_file` must pick the actual audio media, never
a coexisting `.info.json`, `.json3`, or `.part` artifact.
"""

from pathlib import Path

from maia.media.streamer import _find_audio_file


def test_find_audio_file_prefers_media_over_metadata(tmp_path: Path) -> None:
    vid = "ABC123"
    (tmp_path / f"{vid}.webm").write_bytes(b"audio-bytes")
    meta = tmp_path / "meta"
    meta.mkdir()
    (meta / f"{vid}.info.json").write_text("{}")
    # A stray caption + partial download must be ignored.
    (tmp_path / f"{vid}.json3").write_bytes(b"captions")
    (tmp_path / f"{vid}.webm.part").write_bytes(b"partial")

    found = _find_audio_file(tmp_path, vid)
    assert found is not None
    assert found.name == f"{vid}.webm"
    assert found.suffix == ".webm"


def test_find_audio_file_returns_none_when_only_metadata(tmp_path: Path) -> None:
    vid = "XYZ789"
    (tmp_path / f"{vid}.info.json").write_text("{}")
    (tmp_path / f"{vid}.part").write_bytes(b"partial")
    assert _find_audio_file(tmp_path, vid) is None


def test_find_audio_file_ignores_other_videos(tmp_path: Path) -> None:
    vid = "VID1"
    (tmp_path / "OTHER_zzz.webm").write_bytes(b"x")
    (tmp_path / f"{vid}.m4a").write_bytes(b"audio")
    found = _find_audio_file(tmp_path, vid)
    assert found is not None
    assert found.name == f"{vid}.m4a"
