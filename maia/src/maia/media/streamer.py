"""Shared YouTube stream machinery for Maia (Painter + Scribe).

Every agent that touches a YouTube media/caption stream must solve YouTube's
BotGuard/PoToken challenge the same way, so this module is the single source of
truth for the yt-dlp + Deno invocation — the anti-rate-limit flags
(``--js-runtimes deno:...``, ``--remote-components ejs:github``,
``--impersonate``) live in exactly one place and cannot drift.
"""

import json
import logging
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import yt_dlp
from atlas.egress import EgressPool
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

# Shared egress-IP pool (rotated across the Scribe, Streamer, Singer, Painter).
_EGRESS_POOL = EgressPool()

# Sentinel so callers can omit *proxy* but we can still distinguish "not passed"
# from an explicit ``None`` (direct egress).
_UNSET = object()

_YOUTUBE_URL = "https://www.youtube.com/watch?v={video_id}"

# Common yt-dlp base: Python-module invocation + PoToken (Deno) + impersonation.
_YTDLP_BASE = [
    sys.executable,
    "-m",
    "yt_dlp",
    "--quiet",
    "--no-warnings",
    "--remote-components",
    "ejs:github",
    "--impersonate",
    "Chrome-131",
]

# yt-dlp 2026 renamed --js-runtime -> --js-runtimes and requires Deno >= 2.3.0;
# this is the single place that fact is encoded.
_JS_RUNTIME: list[str] = []
for _candidate in (
    shutil.which("deno"),
    "/home/ubuntu/.deno/bin/deno",
    shutil.which("node"),
):
    if _candidate and Path(_candidate).exists():
        _JS_RUNTIME = ["--js-runtimes", f"deno:{_candidate}"]
        break


class StreamRateLimitError(Exception):
    """Raised on yt-dlp rate-limit (HTTP 429 / bot check).

    Deliberately NOT a subclass of ``DownloadError`` so the tenacity retry on
    ``extract_info`` does not re-hammer YouTube; the caller releases the video
    back to PENDING for a later cycle instead.
    """


class AudioExtractionError(Exception):
    """Raised when audio extraction fails."""


class VideoExtractionError(Exception):
    """Raised when full-video extraction (muralist) fails."""


class TranscriptExtractionError(Exception):
    """Raised when transcript extraction fails or returns empty data."""


class TranscriptRateLimitError(TranscriptExtractionError):
    """Raised on yt-dlp rate-limit (HTTP 429 / bot check).

    Distinct from :class:`TranscriptExtractionError` so the flow can release the
    video back to PENDING for a later retry instead of marking it as having no
    transcript.
    """


def _resolve_cookies(cookies_path: str | Path | None) -> list[str]:
    if cookies_path:
        return ["--cookies", str(cookies_path)]
    return []


def yt_dlp_base(
    cookies_path: str | Path | None = None,
    extra: list[str] | None = None,
    proxy: str | None = cast(str | None, _UNSET),
) -> list[str]:
    """Return the common yt-dlp argument list (module + PoToken + impersonation).

    Honours the shared egress-IP pool (``atlas.egress``) so all YouTube-bound
    agents rotate across the same set of egress IPs. *proxy* may be passed
    explicitly (e.g. the next IP after a throttle); when left as the sentinel the
    next IP is taken round-robin from the pool. ``None`` means direct VPS egress.
    """
    cmd = list(_YTDLP_BASE) + _JS_RUNTIME + _resolve_cookies(cookies_path)
    if proxy is _UNSET:
        _, proxy = _EGRESS_POOL.cycle()
    if proxy:
        cmd += ["--proxy", proxy]
    if extra:
        cmd += extra
    return cmd


def _find_json3_file(tmpdir: str, video_id: str) -> Path | None:
    """Return the first JSON3 subtitle file matching *video_id* in *tmpdir*."""
    for f in Path(tmpdir).iterdir():
        if f.suffix in (".json3", ".json") and video_id in f.stem:
            return f
    return None


# yt-dlp side artifacts (metadata JSON, partial downloads, captions) routinely
# coexist with the real audio file; a naive "any file containing the id" scan
# would wrongly point the singer at JSON/partial data, so they are excluded.
_METADATA_EXTS = {
    ".json",
    ".json3",
    ".info",
    ".part",
    ".tmp",
    ".ytdl",
    ".jpg",
    ".png",
    ".vtt",
    ".srt",
    ".sbv",
    ".ass",
}
# Container extensions that can carry the audio stream we want.
_AUDIO_MEDIA_EXTS = {
    ".webm",
    ".m4a",
    ".opus",
    ".mp3",
    ".aac",
    ".ogg",
    ".oga",
    ".flac",
    ".mka",
    ".wav",
    ".mp4",
    ".mkv",
}


def _find_audio_file(dest_dir: str | Path, video_id: str) -> Path | None:
    """Robustly locate the downloaded audio artifact in *dest_dir*.

    Ignores yt-dlp metadata (``.info.json``, ``.json3``), partial downloads
    (``.part``) and other side artifacts so the singer is never pointed at
    non-audio data. Returns ``None`` only when no media file is present (caller
    then treats the fetch as failed and releases the video for retry).
    """
    candidates = [
        f
        for f in Path(dest_dir).iterdir()
        if f.is_file()
        and video_id in f.stem
        and f.suffix.lower() not in _METADATA_EXTS
        and f.suffix.lower() in _AUDIO_MEDIA_EXTS
    ]
    if not candidates:
        return None
    # Deterministic selection.
    candidates.sort(key=lambda p: p.name)
    return candidates[0]


def _parse_json3_file(path: Path) -> list[dict[str, Any]]:
    """Parse a yt-dlp JSON3 subtitle file into ``[{text, start, duration}]``."""
    data = json.loads(path.read_text(encoding="utf-8"))
    segments: list[dict[str, Any]] = []
    for ev in data.get("events", []):
        start = ev.get("tStartMs", 0.0)
        dur = ev.get("dDurationMs", 0.0)
        segs = ev.get("segs") or []
        text = "".join(s.get("utf8", "") for s in segs).strip()
        if text:
            segments.append(
                {
                    "text": text,
                    "start": start / 1000.0,
                    "duration": dur / 1000.0,
                }
            )
    return segments


class StealthVideoStreamer:
    """YouTube streamer using authenticated yt-dlp extraction.

    Cookie authentication is resolved from ``atlas.config.settings`` at
    construction time. Direct ``os.environ`` access is forbidden.
    """

    def __init__(self, cookies_path: str | None = None) -> None:
        if cookies_path is not None:
            resolved: str | None = cookies_path
        else:
            from atlas.config import settings

            resolved = settings.youtube_cookies_resolved_path

        self.cookies_path: Path | None = None
        if resolved:
            p = Path(resolved)
            if p.exists():
                self.cookies_path = p
            else:
                logger.warning(f"Cookies file not found: {p}. Proceeding without auth.")

    def _build_cmd(self, video_url: str) -> list[str]:
        cmd = yt_dlp_base(self.cookies_path) + ["--dump-json", "--no-download"]
        cmd.append(video_url)
        return cmd

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=30),
        retry=retry_if_exception_type(yt_dlp.utils.DownloadError),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def extract_info(self, video_url: str) -> dict[str, Any]:
        if not video_url.startswith("http"):
            video_url = f"https://www.youtube.com/watch?v={video_url}"

        cmd = self._build_cmd(video_url)
        logger.info(f"[yt-dlp] Extracting: {video_url}")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        except subprocess.TimeoutExpired as e:
            raise yt_dlp.utils.DownloadError("yt-dlp timed out after 30s") from e

        if result.returncode != 0:
            stderr = result.stderr.strip()
            from maia.utils import looks_like_rate_limit

            if looks_like_rate_limit(stderr):
                logger.warning(f"[yt-dlp] Rate-limit detected for {video_url}: {stderr[:120]}")
                raise StreamRateLimitError(f"yt-dlp rate-limited for {video_url}: {stderr[:120]}")
            raise yt_dlp.utils.DownloadError(stderr or f"yt-dlp exited {result.returncode}")

        info: dict[str, Any] = json.loads(result.stdout)
        return info

    def extract_heatmap_peaks(
        self, heatmap_data: list[dict[str, Any]], top_n: int = 5
    ) -> list[float]:
        if not heatmap_data:
            return []

        valid_points = [p for p in heatmap_data if "value" in p and "start_time" in p]
        sorted_points = sorted(valid_points, key=lambda x: x.get("value", 0), reverse=True)
        return [p.get("start_time", 0.0) for p in sorted_points[:top_n]]

    def extract_audio(self, video_id: str, dest_dir: str, player_clients: str = "web,tv") -> Path:
        """Download *video_id*'s audio into *dest_dir* and return the opus path.

        Speech-optimised: mono, 16 kHz, 32 kbps opus (~1 MB / 5 min).
        """
        cmd = yt_dlp_base(
            self.cookies_path,
            extra=[
                "--extractor-args",
                f"youtube:player_client={player_clients}",
                "-f",
                "bestaudio/best",
                "--extract-audio",
                "--audio-format",
                "opus",
                "--postprocessor-args",
                "ffmpeg:-ac 1 -ar 16000 -b:a 32k",
                "-o",
                f"{dest_dir}/%(id)s.%(ext)s",
            ],
        )
        cmd.append(_YOUTUBE_URL.format(video_id=video_id))
        logger.info(f"[yt-dlp] Extracting audio: {video_id}")

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if result.returncode != 0:
            stderr = result.stderr.strip()
            raise AudioExtractionError(f"Audio extraction failed for {video_id}: {stderr[:200]}")

        for f in Path(dest_dir).iterdir():
            if f.suffix == ".opus" and video_id in f.stem:
                return f
        raise AudioExtractionError(f"yt-dlp exited OK but no audio file found for {video_id}")

    def download_unified(
        self,
        video_id: str,
        dest_dir: str,
        player_clients: str = "web,tv",
    ) -> tuple[Path, Path | None]:
        """Unified YouTube ingress: download the audio + metadata for *video_id*
        in a SINGLE yt-dlp invocation.

        Produces the audio stream (the singer extracts speech from it locally)
        and ``meta/<id>.info.json`` (every format's stream URL, so the painter
        pulls frames via HTTP range requests from YouTube's CDN). Captions are
        deliberately NOT fetched here: the ``timedtext`` endpoint is the one
        YouTube surface that throttles this VPS's egress IP, and bolting it onto
        the audio download poisons the whole fetch. Returns
        ``(audio_path, info_path | None)``.

        Raises:
            StreamRateLimitError: transient YouTube throttle.
            AudioExtractionError: hard download failure.
        """
        meta_dir = Path(dest_dir) / "meta"
        meta_dir.mkdir(parents=True, exist_ok=True)

        cmd = yt_dlp_base(
            self.cookies_path,
            extra=[
                "--extractor-args",
                f"youtube:player_client={player_clients}",
                # Audio-only: the singer re-encodes to opus locally, so we never
                # download the heavy muxed video (the painter's frame source comes
                # from the stream URLs in the info.json).
                "-f",
                "bestaudio/best",
                "--write-info-json",
                "-o",
                f"{dest_dir}/%(id)s.%(ext)s",
                "--paths",
                f"infojson:{meta_dir}",
            ],
        )
        cmd.append(_YOUTUBE_URL.format(video_id=video_id))
        logger.info(f"[yt-dlp] Unified fetch (audio+meta): {video_id}")

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            stderr = result.stderr.strip()
            from maia.utils import looks_like_rate_limit

            # The audio is the critical artifact; info.json is best-effort (the
            # painter falls back to its own YouTube metadata fetch). If YouTube
            # rate-limits the metadata endpoint, the audio has usually already
            # been downloaded — salvage it instead of failing the whole fetch.
            audio_path = _find_audio_file(dest_dir, video_id)
            if audio_path is None:
                if looks_like_rate_limit(stderr):
                    logger.warning(f"[yt-dlp] Rate-limit for {video_id}: {stderr[:120]}")
                    raise StreamRateLimitError(
                        f"yt-dlp rate-limited for {video_id}: {stderr[:120]}"
                    )
                raise AudioExtractionError(f"Unified fetch failed for {video_id}: {stderr[:200]}")
            logger.warning(
                f"[yt-dlp] Audio salvaged for {video_id} but metadata failed "
                f"(continuing): {stderr[:160]}"
            )
        else:
            audio_path = _find_audio_file(dest_dir, video_id)

        # Locate the produced metadata artifact.
        info_path = next(
            (f for f in meta_dir.iterdir() if f.suffix == ".json" and "info" in f.name),
            None,
        )
        if audio_path is None:
            raise AudioExtractionError(f"yt-dlp exited OK but no audio file found for {video_id}")
        return audio_path, info_path

    def download_raw(self, video_id: str, dest_dir: str, player_clients: str = "web,tv") -> Path:
        """Download *video_id*'s best audio stream (no ffmpeg re-encode).

        This is the network-heavy, rate-limit-prone step. The file keeps its
        native container (e.g. ``.webm``/``.m4a``); the singer later runs ffmpeg
        locally to extract the speech track. Returns the path of the downloaded
        file.
        """
        cmd = yt_dlp_base(
            self.cookies_path,
            extra=[
                "--extractor-args",
                f"youtube:player_client={player_clients}",
                "-f",
                "bestaudio/best",
                "-o",
                f"{dest_dir}/%(id)s.%(ext)s",
            ],
        )
        cmd.append(_YOUTUBE_URL.format(video_id=video_id))
        logger.info(f"[yt-dlp] Fetching raw audio stream: {video_id}")

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if result.returncode != 0:
            stderr = result.stderr.strip()
            from maia.utils import looks_like_rate_limit

            if looks_like_rate_limit(stderr):
                logger.warning(f"[yt-dlp] Rate-limit for {video_id}: {stderr[:120]}")
                raise StreamRateLimitError(f"yt-dlp rate-limited for {video_id}: {stderr[:120]}")
            raise AudioExtractionError(f"Raw fetch failed for {video_id}: {stderr[:200]}")

        for f in sorted(Path(dest_dir).iterdir()):
            if f.is_file() and video_id in f.stem:
                return f
        raise AudioExtractionError(f"yt-dlp exited OK but no file found for {video_id}")

    def extract_video(self, video_id: str, dest_dir: str, height: int = 720) -> Path:
        """Download *video_id*'s source video (<= *height*, no re-encode).

        Prefers YouTube's own pre-encoded progressive stream (or merges the best
        video+audio <= *height*) so no CPU transcode is needed. Used by the
        muralist consumer to archive the full clip to the vault.

        Raises:
            VideoExtractionError: when the download fails or no file is produced.
        """
        # Prefer a progressive file <= height; else merge best video+audio <= height.
        fmt = f"b[height<={height}]/bv[height<={height}]+ba/b[height<={height}]/b"
        cmd = yt_dlp_base(
            self.cookies_path,
            extra=[
                "-f",
                fmt,
                "--merge-output-format",
                "mp4",
                "-o",
                f"{dest_dir}/%(id)s.%(ext)s",
            ],
        )
        cmd.append(_YOUTUBE_URL.format(video_id=video_id))
        logger.info(f"[yt-dlp] Extracting video: {video_id} (<= {height}p)")

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            stderr = result.stderr.strip()
            raise VideoExtractionError(f"Video extraction failed for {video_id}: {stderr[:200]}")

        for f in sorted(Path(dest_dir).iterdir()):
            if f.is_file() and video_id in f.stem:
                return f
        raise VideoExtractionError(f"yt-dlp exited OK but no video file found for {video_id}")

    def extract_captions(self, video_id: str, client: str, tmpdir: str) -> list[dict[str, Any]]:
        """Download JSON3 captions for *video_id* via player *client*.

        Raises:
            TranscriptRateLimitError: transient throttle — caller may try another client.
            TranscriptExtractionError: genuine "no subtitles" / failure for this client.
        """
        cmd = yt_dlp_base(
            self.cookies_path,
            extra=[
                "--skip-download",
                "--write-auto-subs",
                "--sub-format",
                "json3",
                "--sub-langs",
                "en",
                "--extractor-args",
                f"youtube:player_client={client}",
                "--paths",
                f"subtitle:{tmpdir}",
                "--output",
                f"subtitle:{tmpdir}/%(id)s.%(ext)s",
            ],
        )
        cmd.append(_YOUTUBE_URL.format(video_id=video_id))
        logger.info(f"[yt-dlp] Extracting captions [{client}]: {video_id}")

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            stderr = result.stderr.strip()
            from maia.utils import looks_like_rate_limit

            if looks_like_rate_limit(stderr):
                raise TranscriptRateLimitError(
                    f"yt-dlp rate-limited for {video_id} [{client}]: {stderr[:120]}"
                )
            if "subtitles" in stderr.lower() and "not" in stderr.lower():
                raise TranscriptExtractionError(f"No subtitles available for {video_id}")
            raise TranscriptExtractionError(
                f"yt-dlp failed for {video_id} [{client}]: {stderr or result.stdout.strip()}"
            )

        sub_file = _find_json3_file(tmpdir, video_id)
        if sub_file is None:
            raise TranscriptExtractionError(
                f"yt-dlp exited OK but no subtitle file found for {video_id}"
            )

        segments = _parse_json3_file(sub_file)
        if not segments:
            raise TranscriptExtractionError(f"Subtitle file for {video_id} is empty")
        return segments


def extract_audio_ffmpeg(src: Path, dst: Path, timeout: int = 180) -> Path:
    """Extract a speech-optimised opus track from *src* to *dst* via ffmpeg.

    Local CPU work only — no network, so it does not consume YouTube quota.
    Speech-optimised: mono, 16 kHz, 32 kbps opus (~1 MB / 5 min). Used by the
    singer consumer, which operates on the raw artifact the streamer fetched.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(src),
        "-ac",
        "1",
        "-ar",
        "16000",
        "-b:a",
        "32k",
        "-f",
        "opus",
        str(dst),
    ]
    logger.info(f"[ffmpeg] Extracting audio: {dst.name}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise AudioExtractionError(
            f"ffmpeg audio extraction failed for {dst.name}: {result.stderr.strip()[:200]}"
        )
    if not dst.exists():
        raise AudioExtractionError(f"ffmpeg produced no file for {dst.name}")
    return dst


def extract_audio_chunk(
    src: Path, dst: Path, start: float, length: float, timeout: int = 180
) -> Path:
    """Extract a *time-bounded* speech-optimised opus segment from *src*.

    Used to split long videos into short chunks so each ffmpeg call stays under
    the timeout while the full audio track is still captured across chunks.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-y",
        "-ss",
        str(start),
        "-i",
        str(src),
        "-t",
        str(length),
        "-ac",
        "1",
        "-ar",
        "16000",
        "-b:a",
        "32k",
        "-f",
        "opus",
        str(dst),
    ]
    logger.info(f"[ffmpeg] Extracting audio chunk {dst.name} (start={start}s len={length}s)")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise AudioExtractionError(
            f"ffmpeg audio chunk failed for {dst.name}: {result.stderr.strip()[:200]}"
        )
    if not dst.exists():
        raise AudioExtractionError(f"ffmpeg produced no chunk for {dst.name}")
    return dst
