import json
import logging
import tempfile
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, PostgresDsn, SecretStr, model_validator
from pydantic_settings import BaseSettings

logger = logging.getLogger("atlas.config")


class Settings(BaseSettings):  # type: ignore[misc]
    DATABASE_URL: PostgresDsn = Field(..., description="Neon/Postgres Connection String")
    VAULT_PROVIDER: Literal["huggingface", "gcs"] = Field(
        "huggingface", description="Storage Backend Provider"
    )

    HF_DATASET_ID: str | None = Field(None, description="HF Dataset ID (username/dataset)")
    HF_TOKEN: SecretStr | None = Field(None, description="HF Write Token")
    GCS_BUCKET_NAME: str | None = Field(None, description="GCS Bucket Name")

    COMPLIANCE_MODE: bool = Field(
        False,
        description=(
            "Enforce API policy limits. When enabled, Atlas logs a warning and "
            "applies conservative behaviour but does NOT collapse the key pool — "
            "key rotation/resilience is always preserved."
        ),
    )
    ENV: str = Field("dev", description="Deployment environment (dev/prod)")

    YOUTUBE_API_KEY_POOL_JSON: SecretStr = Field(..., description="JSON List of YouTube API Keys")

    KEY_POOL_ARCHEOLOGY_SIZE: int = Field(1, description="Keys reserved for archeology")
    KEY_POOL_TRACKING_SIZE: int = Field(1, description="Keys reserved for tracking")
    KEY_POOL_GRAPHER_SIZE: int = Field(1, ge=1, description="Exclusive keys reserved for Grapher")

    DISCORD_WEBHOOK_ALERTS: SecretStr | None = None
    DISCORD_WEBHOOK_HUNT: SecretStr | None = None
    DISCORD_WEBHOOK_SURVEILLANCE: SecretStr | None = None
    DISCORD_WEBHOOK_OPS: SecretStr | None = None

    PREFECT_API_URL: str | None = None
    PREFECT_API_KEY: SecretStr | None = None

    JANITOR_ENABLED: bool = Field(
        True, description="Enable automatic cleanup of old processed data"
    )
    TOPIC_SYNC_ENABLED: bool = Field(
        True, description="Enrich existing videos/channels with YouTube topics"
    )
    JANITOR_RETENTION_DAYS: int = Field(7, description="Days to retain processed data in hot queue")
    JANITOR_METRICS_RETENTION_DAYS: int = Field(
        2, ge=1, le=90, description="Days of detailed metrics to retain in PostgreSQL"
    )
    JANITOR_TRANSCRIPT_BATCH_SIZE: int = Field(100, ge=1, le=500)
    JANITOR_TRANSCRIPT_MAX_BATCHES: int = Field(10, ge=1, le=50)
    JANITOR_TRANSCRIPT_BUDGET_SECONDS: int = Field(120, ge=1, le=600)
    JANITOR_SAFETY_CHECK: bool = Field(
        True, description="Verify data exists in Vault before deletion"
    )

    RAW_TTL_HOURS: int = Field(
        48,
        description=(
            "Hours to retain the raw source artifact after fetch before "
            "reclaiming it when the muralist (clip producer) has not run. "
            "Singer and painter must finish first (has_audio AND has_visuals); "
            "only then is the raw eligible for reclamation — immediately once "
            "has_video is set, or after this TTL if the muralist never runs "
            "(keeps disk bounded on a VPS where muralist is manual-only)."
        ),
    )

    YOUTUBE_COOKIES_PATH: str | None = Field(
        None, description="Path to Netscape cookies.txt file for YouTube authentication"
    )
    YOUTUBE_COOKIES_CONTENT: SecretStr | None = Field(
        None,
        description="Raw Netscape cookies.txt content (written to temp file at startup)",
    )

    MISTRAL_API_KEY: SecretStr | None = Field(
        None, description="Mistral API key for Voxtral speech-to-text transcription"
    )
    GROK_API_KEY: SecretStr | None = Field(
        None,
        description=(
            "Groq Cloud API key (console.groq.com, 'gsk_…') for Whisper "
            "speech-to-text transcription (Groq, not xAI)."
        ),
    )
    SCRIBE_DAILY_AUDIO_CAP: int = Field(
        30,
        description=(
            "Max videos/day transcribed via paid audio fallback (Grok/Mistral). "
            "Once hit, the scribe falls back to captions-only for the day."
        ),
    )
    SCRIBE_TRANSCRIBER: Literal["auto", "captions", "mistral", "grok"] = Field(
        "auto",
        description=(
            "Transcription strategy: 'captions' (yt-dlp caption cascade only), "
            "'mistral' (audio → Voxtral only), 'grok' (audio → Grok STT only), or "
            "'auto' (captions first, fall back to audio→Grok then audio→Mistral on "
            "rate-limit / no-captions)."
        ),
    )
    SCRIBE_STORE_AUDIO: bool = Field(
        False,
        description="Persist the extracted audio (opus) to the vault after transcription.",
    )

    QUALITY_GATE_ENABLED: bool = Field(
        True, description="Reject low-value videos before ingest / snowball."
    )
    QUALITY_MIN_DURATION_SECONDS: int = Field(
        65, description="Reject videos shorter than this (filters Shorts)."
    )
    QUALITY_MIN_VIEWS_PER_HOUR: float = Field(
        5.0, description="Minimum views/hour since publication (traction)."
    )
    QUALITY_MIN_ENGAGEMENT_RATE: float = Field(
        0.005, description="Minimum (likes+comments)/views engagement ratio."
    )

    # The Data API has no `isShort` flag, so we HEAD-probe /shorts/{id} (200=Short, 3xx=long-form).
    QUALITY_SHORTS_HEAD_ENABLED: bool = Field(
        True,
        description="Detect Shorts via HEAD probe to /shorts/{id} (200=Short, 3xx=long-form).",
    )
    QUALITY_SHORTS_HEAD_MAX_DURATION: int = Field(
        600,
        description="Only HEAD-probe candidates shorter than this (saves probes on long videos).",
    )
    QUALITY_SHORTS_HEAD_TIMEOUT: float = Field(5.0, description="Per-probe HTTP timeout (seconds).")
    QUALITY_SHORTS_HEAD_CONCURRENCY: int = Field(
        8, description="Max concurrent HEAD probes per batch."
    )

    # No API flag marks AI-generated videos, so we match conservative regexes on
    # title/description/tags (tunable; empty disables).
    QUALITY_AI_DENYLIST: list[str] = Field(
        default_factory=lambda: [
            r"\b(ai[- ]?generated|ai[- ]?created|ai[- ]?made)\b",
            r"\b(made|created|generated) (with|using) ai\b",
            r"\b(this (video|content) (was|is) (generated|created|made) (with|using) ai)\b",
            r"\b(fully (generated|created) by ai)\b",
            r"\b(text[- ]?to[- ]?speech|tts voice|ai voiceover|ai narration|"
            r"synthetic (voice|narration|media))\b",
        ],
        description="Regexes (title/description/tags) indicating AI-generated content.",
    )

    QUALITY_MIN_SUBSCRIBERS: int = Field(
        50,
        description=(
            "Reject videos from channels with fewer subscribers than this "
            "(catches tiny AI-farm/spam channels). 0 disables."
        ),
    )
    QUALITY_MAX_VIDEOS_PER_DAY: float = Field(
        20.0,
        description=(
            "Reject a video if its channel's upload rate (videoCount / channel_age_days) "
            "exceeds this (mass-upload channels). Set high (or 0) so legitimate networks "
            "that post many Shorts are NOT filtered; pair with QUALITY_MAX_VIDEOS_PER_SUBSCRIBER "
            "for the actual spam signal. 0 disables."
        ),
    )
    QUALITY_MAX_VIDEOS_PER_SUBSCRIBER: float = Field(
        5.0,
        description=(
            "Better AI-farm signal than raw upload rate: reject a video if its channel's "
            "videoCount / subscriberCount exceeds this (i.e. far more videos than subscribers "
            "— the hallmark of spam/AI farms). Legit high-volume networks have many subscribers "
            "per video, so they pass. 0 disables."
        ),
    )

    # Re-upload detection: matches emojified network names (e.g. 🅼🆂🅽🅱🅲 for
    # MSNBC, 🅱🅱🅲 for BBC) in video titles — a hallmark of copyright-infringing
    # re-upload channels that get DMCA'd within hours. The default matches any
    # 2+ consecutive characters from the Enclosed Alphanumeric Supplement block
    # (U+1F170-U+1F1FF: Negative Squared / Squared Latin letters).
    QUALITY_REUPLOAD_DENYLIST: list[str] = Field(
        default_factory=lambda: [
            r"[\U0001F170-\U0001F1FF]{2,}",
        ],
        description="Regexes (title/description/tags) indicating copyrighted re-uploaded content.",
    )

    SEARCH_QUEUE_MENTION_WEIGHT: float = Field(
        1.5, description="Weight applied to mention_count in the queue score."
    )
    SEARCH_QUEUE_DECAY_PER_HOUR: float = Field(
        0.1, description="Score decay per hour a term has sat in the queue."
    )
    SEARCH_QUEUE_CULL_BELOW: float = Field(
        0.0, description="Janitor deletes queue terms whose score drops below this."
    )

    # ── Adaptive scheduling (Tracker decay) ──────────────────────────────────
    # Tier by age (baseline) then adjust by view velocity (views/hour measured
    # across the video's two most recent video_stats_log samples).
    #   * velocity >= HOT  → boosted to HOURLY (stays hourly past the age cutoffs)
    #   * velocity <= DEAD → dropped one tier faster (dead videos recede sooner)
    #   * otherwise        → age floor applies unchanged
    # A video is never tracked more often than hourly and never less than weekly.
    TRACKER_AGE_HOURLY_HOURS: float = Field(
        24.0,
        description="Videos published within this many hours are tracked hourly.",
    )
    TRACKER_AGE_DAILY_DAYS: float = Field(
        7.0,
        description="Videos published within this many days are tracked daily.",
    )
    TRACKER_HOT_VIEWS_PER_HOUR: float = Field(
        50.0, description="Views/hour above which a video is boosted to HOURLY tracking."
    )
    TRACKER_DEAD_VIEWS_PER_HOUR: float = Field(
        1.0,
        description="Views/hour at/below which a video drops one tier faster.",
    )

    @model_validator(mode="after")
    def validate_vault_config(self) -> "Settings":
        if self.VAULT_PROVIDER == "huggingface":
            if not self.HF_DATASET_ID or not self.HF_TOKEN:
                raise ValueError("HF_DATASET_ID and HF_TOKEN required for HuggingFace vault")
        elif self.VAULT_PROVIDER == "gcs" and not self.GCS_BUCKET_NAME:
            raise ValueError("GCS_BUCKET_NAME required for GCS vault")
        return self

    @property
    def api_keys(self) -> list[str]:
        try:
            payload = self.YOUTUBE_API_KEY_POOL_JSON.get_secret_value()
            keys_parsed = json.loads(payload)
            if isinstance(keys_parsed, str):
                keys_list: list[str] = [keys_parsed]
            else:
                keys_list = keys_parsed

            if self.COMPLIANCE_MODE:
                # Compliance mode must never collapse the key pool — that would
                # disable rotation and guarantee quota exhaustion on the first 429.
                logger.warning(
                    "COMPLIANCE_MODE is enabled: applying conservative API policy "
                    "enforcement, but all keys remain available for rotation."
                )
            return keys_list
        except json.JSONDecodeError:
            return [self.YOUTUBE_API_KEY_POOL_JSON.get_secret_value()]

    def effective_pool_sizes(self) -> tuple[int, int]:
        """Return ``(tracking_size, archeology_size)`` in effect.

        A cached dynamic allocation (written weekly by the janitor from the corpus
        size) takes precedence; otherwise the static ``.env`` sizes are used. Reading
        the cache keeps config free of any database dependency.
        """
        from atlas.key_pool import load_override

        override = load_override()
        if override is not None:
            return override.tracking, override.archeology
        return self.KEY_POOL_TRACKING_SIZE, self.KEY_POOL_ARCHEOLOGY_SIZE

    @property
    def key_rings(self) -> dict[str, list[str]]:
        # Deduplicate before assigning rings so Grapher's key cannot leak into
        # another pool through a repeated credential in the configured list.
        all_keys = list(dict.fromkeys(self.api_keys))
        grapher_count = min(self.KEY_POOL_GRAPHER_SIZE, max(0, len(all_keys) - 1))
        grapher_keys = all_keys[-grapher_count:] if grapher_count else []
        raw_keys = all_keys[: len(all_keys) - grapher_count]
        total_keys = len(raw_keys)
        tracking_size, archeology_size = self.effective_pool_sizes()
        # Scheduled discovery/tracking take precedence over manual Archeologist.
        # Clamp old cached allocations after reserving Grapher's exclusive slice.
        archeology_size = min(max(0, archeology_size), max(0, total_keys - 2))
        tracking_size = min(max(1, tracking_size), max(1, total_keys - archeology_size - 1))
        reserved_count = archeology_size + tracking_size

        if total_keys <= reserved_count:
            logger.warning(
                f"Config: Insufficient keys for strict pooling! "
                f"Need > {reserved_count}, got {total_keys}. "
                "Main pools share available keys; Grapher's reserve stays exclusive."
            )
            return {
                "hunting": raw_keys,
                "tracking": raw_keys,
                "archeology": raw_keys,
                "grapher": grapher_keys,
            }

        archeology_keys = raw_keys[-archeology_size:] if archeology_size else []
        remaining = raw_keys[: total_keys - archeology_size]

        # Hunter keeps the bulk (its 100/day search.list bucket rate-limits
        # first); tracking is the cheap videos.list ring with a protected slice.
        hunting_size = total_keys - tracking_size - archeology_size
        hunting_keys = remaining[:hunting_size]
        tracking_keys = remaining[hunting_size:]

        return {
            "grapher": grapher_keys,
            "hunting": hunting_keys,
            "tracking": tracking_keys,
            "archeology": archeology_keys,
        }

    @property
    def youtube_cookies_resolved_path(self) -> str | None:
        """Resolve the YouTube cookies file path.

        Priority:
        1. ``YOUTUBE_COOKIES_PATH`` — explicit file path.
        2. ``YOUTUBE_COOKIES_CONTENT`` — raw content materialised to a temp file.
        3. ``None`` — no cookies configured.
        """
        if self.YOUTUBE_COOKIES_PATH:
            p = Path(self.YOUTUBE_COOKIES_PATH)
            if p.exists():
                return str(p)
            logger.warning(f"YOUTUBE_COOKIES_PATH set but file missing: {p}")
            return None

        if self.YOUTUBE_COOKIES_CONTENT:
            content = self.YOUTUBE_COOKIES_CONTENT.get_secret_value()
            tmp = Path(tempfile.gettempdir()) / "youtube_cookies.txt"
            tmp.write_text(content)
            return str(tmp)

        return None

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": True,
        "extra": "ignore",
    }


_settings_instance: Settings | None = None


def get_settings() -> Settings:
    """Lazily initialised settings singleton.

    ``from atlas.config import settings`` triggers ``__getattr__`` below which
    calls this on first access, so no env vars are read or validated at import
    time. Tests can replace ``_settings_instance`` or call ``reset_settings()``
    beforehand.
    """
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()  # type: ignore[call-arg]
    return _settings_instance


def reset_settings() -> None:
    """Clear the cached settings singleton (useful for testing)."""
    global _settings_instance
    _settings_instance = None


def __getattr__(name: str) -> Any:
    if name == "settings":
        return get_settings()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
