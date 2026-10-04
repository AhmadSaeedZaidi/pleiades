"""Audio extraction for speech-to-text transcription.

Downloads a compact, speech-optimised audio track via the shared
:class:`~maia.media.streamer.StealthVideoStreamer`.
"""

import logging
import shutil
import tempfile
from pathlib import Path

from maia.media.streamer import AudioExtractionError, StealthVideoStreamer

logger = logging.getLogger(__name__)

__all__ = ["AudioExtractionError", "AudioLoader", "download_to_tempfile"]


class AudioLoader:
    """Download and transcode a video's audio track to compact opus."""

    def __init__(self, cookies_path: str | None = None) -> None:
        self.cookies_path = cookies_path
        self.logger = logging.getLogger("maia.scribe.audio")

    def download(self, video_id: str, dest_dir: str) -> Path:
        """Download *video_id*'s audio into *dest_dir* and return the opus path.

        Raises:
            AudioExtractionError: when the download/transcode fails.
        """
        return StealthVideoStreamer(self.cookies_path).extract_audio(video_id, dest_dir)


def download_to_tempfile(video_id: str, cookies_path: str | None = None) -> tuple[Path, str]:
    """Download audio to a fresh temp dir; caller must clean up the dir.

    Returns ``(audio_path, tmpdir)``.
    """
    tmpdir = tempfile.mkdtemp(prefix="scribe-audio-")
    try:
        path = AudioLoader(cookies_path).download(video_id, tmpdir)
        return path, tmpdir
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise
