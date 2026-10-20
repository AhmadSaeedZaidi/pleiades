"""Tests for memory-bounded vault materialization."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from atlas.vault import GCSVault, HuggingFaceVault
from huggingface_hub.errors import EntryNotFoundError, LocalEntryNotFoundError


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
        with pytest.raises(RuntimeError, match="download interrupted"):
            vault.fetch_binary_to_path("raw/v.webm", destination)
    assert destination.read_bytes() == b"existing"
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.parametrize(
    "error", [OSError(28, "No space left on device"), LocalEntryNotFoundError("offline cache miss")]
)
def test_local_download_failures_are_not_remote_absence(tmp_path, error):
    vault = object.__new__(HuggingFaceVault)
    vault.repo_id, vault.token = "owner/dataset", "token"
    with patch("atlas.vault.hf_hub_download", side_effect=error), pytest.raises(type(error)):
        vault.fetch_binary_to_path("raw/v.webm", tmp_path / "video.webm")


def test_confirmed_missing_huggingface_object_returns_false(tmp_path):
    vault = object.__new__(HuggingFaceVault)
    vault.repo_id, vault.token = "owner/dataset", "token"
    with patch("atlas.vault.hf_hub_download", side_effect=EntryNotFoundError("missing")):
        assert not vault.fetch_binary_to_path("raw/v.webm", tmp_path / "video.webm")


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


def test_gcs_download_errors_propagate_instead_of_claiming_absence(tmp_path):
    vault = object.__new__(GCSVault)
    vault.bucket = MagicMock()
    vault.bucket.blob.return_value.exists.return_value = True
    vault.bucket.blob.return_value.download_to_filename.side_effect = OSError(28, "Disk full")
    with pytest.raises(OSError):
        vault.fetch_binary_to_path("raw/v.webm", tmp_path / "video.webm")


def test_gcs_confirmed_absence_returns_false(tmp_path):
    vault = object.__new__(GCSVault)
    vault.bucket = MagicMock()
    vault.bucket.blob.return_value.exists.return_value = False
    assert not vault.fetch_binary_to_path("raw/v.webm", tmp_path / "video.webm")
    vault.bucket.blob.return_value.download_to_filename.assert_not_called()
