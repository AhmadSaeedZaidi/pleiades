"""Tests for memory-bounded vault materialization."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from atlas.vault import GCSVault, HuggingFaceVault


def test_huggingface_media_uses_temporary_download_and_cleans_it(tmp_path: Path) -> None:
    destination = tmp_path / "work" / "video.webm"
    destination.parent.mkdir()
    vault = object.__new__(HuggingFaceVault)
    vault.repo_id, vault.token = "owner/dataset", "token"
    downloads = []

    def materialize(**kwargs):
        folder = Path(kwargs["local_dir"])
        downloads.append(folder)
        path = folder / "raw" / "v.webm"
        path.parent.mkdir()
        path.write_bytes(b"raw-media")
        return str(path)

    with patch("atlas.vault.hf_hub_download", side_effect=materialize) as download:
        assert vault.fetch_binary_to_path("hf://datasets/owner/dataset/raw/v.webm", destination)
    assert destination.read_bytes() == b"raw-media"
    assert not downloads[0].exists()
    assert download.call_args.kwargs["filename"] == "raw/v.webm"
    assert "cache_dir" not in download.call_args.kwargs


def test_huggingface_failed_download_cleans_temporary_data(tmp_path: Path) -> None:
    vault = object.__new__(HuggingFaceVault)
    vault.repo_id, vault.token = "owner/dataset", "token"
    destination = tmp_path / "video.webm"
    destination.write_bytes(b"existing")

    def interrupted(**kwargs):
        (Path(kwargs["local_dir"]) / "partial").write_bytes(b"incomplete")
        raise RuntimeError("download interrupted")

    with patch("atlas.vault.hf_hub_download", side_effect=interrupted):
        assert not vault.fetch_binary_to_path("raw/v.webm", destination)
    assert destination.read_bytes() == b"existing"
    assert list(tmp_path.iterdir()) == [destination]


def test_gcs_materializes_directly_to_filename(tmp_path: Path) -> None:
    destination = tmp_path / "video.webm"

    blob = MagicMock()
    blob.exists.return_value = True
    blob.download_to_filename.side_effect = lambda path: Path(path).write_bytes(b"raw-media")
    bucket = MagicMock()
    bucket.blob.return_value = blob

    vault = object.__new__(GCSVault)
    vault.bucket_name = "bucket"
    vault.bucket = bucket

    assert vault.fetch_binary_to_path("gs://bucket/raw/v.webm", destination)
    assert destination.read_bytes() == b"raw-media"
