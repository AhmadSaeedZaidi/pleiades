"""Utility functions for Atlas infrastructure."""

import itertools
import json
import logging
from collections.abc import Callable, Sequence
from typing import Any, Literal, TypeVar

logger = logging.getLogger("atlas.utils")

T = TypeVar("T")


class QuotaExhaustedError(Exception):
    """Raised when all API keys in a KeyRing are exhausted for a request.

    A normal ``Exception`` (not ``SystemExit``) so callers decide the resiliency
    action and it never tears down an unrelated host process importing Atlas.
    """


class YouTubeAPIError(Exception):
    """Structured error returned by a YouTube Data API request."""

    def __init__(self, status: int, reason: str | None, message: str) -> None:
        self.status = status
        self.reason = reason
        self.message = message
        detail = f" [{reason}]" if reason else ""
        super().__init__(f"YouTube API error {status}{detail}: {message}")

    @classmethod
    def from_response(cls, status: int, body: str) -> "YouTubeAPIError":
        """Build an error while retaining the API's status, reason and message."""
        reason: str | None = None
        message = body[:200] or f"HTTP {status}"
        try:
            payload: Any = json.loads(body)
        except (TypeError, ValueError):
            payload = None

        error = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error, dict):
            top_level_message = error.get("message")
            if isinstance(top_level_message, str) and top_level_message:
                message = top_level_message
            details = error.get("errors")
            if isinstance(details, list):
                for detail in details:
                    if not isinstance(detail, dict):
                        continue
                    candidate_reason = detail.get("reason")
                    if isinstance(candidate_reason, str) and candidate_reason:
                        reason = candidate_reason
                        break

        return cls(status, reason, message)


ErrorDisposition = Literal["dead_key", "quota", "retryable", "fatal"]


def validate_youtube_id(video_id: str) -> bool:
    """Validate an 11-char base64url YouTube video ID."""
    if not video_id or not isinstance(video_id, str):
        return False

    if len(video_id) != 11:
        return False

    allowed_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
    return all(c in allowed_chars for c in video_id)


def validate_channel_id(channel_id: str) -> bool:
    """Validate a 24-char UC-prefixed YouTube channel ID."""
    if not channel_id or not isinstance(channel_id, str):
        return False

    if not channel_id.startswith("UC"):
        return False

    if len(channel_id) != 24:
        return False

    allowed_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
    return all(c in allowed_chars for c in channel_id)


class KeyRing:
    """Manages API key rotation with exhaustible session tracking."""

    def __init__(self, pool_name: str, *, keys: Sequence[str] | None = None):
        if keys is None:
            from atlas.config import settings

            keys = settings.key_rings.get(pool_name.lower(), [])

        self.pool_name = pool_name.lower()
        self.keys: list[str] = list(keys)

        if not self.keys:
            logger.error(f"KeyRing: No keys initialized for pool '{pool_name}'!")
            raise ValueError(f"Empty KeyRing for {pool_name}")

        self._iterator: itertools.cycle[str] = itertools.cycle(self.keys)

        # Keys observed to fail with an explicit unrecoverable key error (e.g. a
        # revoked API key). Blacklisted keys are never handed out again within
        # this process so a few dead keys can't wedge the whole ring. The ring
        # recovers automatically as soon as at least one live key remains; if
        # every key is blacklisted the ring is genuinely exhausted.
        self._dead_keys: set[str] = set()
        self._live_keys: list[str] = list(self.keys)

        # A monotonic counter guarantees unique session ids so concurrent
        # calls never clobber each other's attempt counts.
        self._session_counter = itertools.count()
        self._current_session_attempts: dict[int, int] = {}
        self._session_key_orders: dict[int, list[str]] = {}
        self._session_seen_keys: dict[int, set[str]] = {}

        logger.info(f"KeyRing: Initialized '{pool_name}' with {len(self.keys)} keys.")

    @classmethod
    def from_keys(cls, pool_name: str, keys: Sequence[str]) -> "KeyRing":
        """Build a ring from an explicit key sequence rather than Atlas settings.

        This is intended for isolated consumers such as an MCP reserve pool;
        regular application rings should continue to use ``KeyRing(pool_name)``.
        """
        return cls(pool_name, keys=keys)

    def next_key(self) -> str:
        """Get the next live key from the rotation.

        Blacklisted keys are skipped. This used to be a bare ``next(self._iterator)``
        over a cycle built from ``self.keys`` at construction time, so
        ``mark_key_dead`` had no effect on it and a revoked key kept being handed
        out for the lifetime of the process — the ring had two disagreeing notions
        of "usable keys" and nothing reconciled them.
        """
        for _ in range(len(self.keys)):
            candidate = next(self._iterator)
            if candidate not in self._dead_keys:
                return candidate
        # Every key in the cycle is blacklisted.
        raise QuotaExhaustedError(
            f"No live keys remain in pool '{self.pool_name}' "
            f"({len(self._dead_keys)}/{len(self.keys)} blacklisted)"
        )

    def mark_key_dead(self, key: str) -> None:
        """Permanently exclude ``key`` from this ring for the process lifetime.

        Called only when the provider identifies this exact key as invalid or
        expired. Quota and rate-limit errors are transient and must not call
        this method.
        """
        if key in self._dead_keys:
            return
        self._dead_keys.add(key)
        self._live_keys = [k for k in self.keys if k not in self._dead_keys]
        logger.warning(
            f"KeyRing '{self.pool_name}': blacklisted dead key {key[-6:]} "
            f"({len(self._live_keys)}/{len(self.keys)} still live)"
        )

    @property
    def live_size(self) -> int:
        """Number of keys still considered usable."""
        return len(self._live_keys)

    def start_session(self, session_id: int | None = None) -> int:
        """Start a new exhaustible rotation session; allocates a unique id if
        none given. Returns the session id."""

        if session_id is None:
            session_id = next(self._session_counter)
        self._current_session_attempts[session_id] = 0
        return session_id

    def get_session_key(self, session_id: int) -> str:
        """Return the next key for this session, skipping blacklisted keys."""
        if session_id not in self._session_key_orders:
            self._session_key_orders[session_id] = list(self._live_keys)
            self._session_seen_keys[session_id] = set()

        seen = self._session_seen_keys[session_id]
        for key in self._session_key_orders[session_id]:
            if key in self._live_keys and key not in seen:
                seen.add(key)
                return key

        raise QuotaExhaustedError(f"No live keys remain for {self.pool_name}")

    def attempt_rotation(self, session_id: int) -> bool:
        """Rotate to the next key; return True if more live keys remain, False if exhausted."""
        if session_id not in self._current_session_attempts:
            logger.warning(f"Session {session_id} not found, initializing")
            self._current_session_attempts[session_id] = 0

        self._current_session_attempts[session_id] += 1

        seen = self._session_seen_keys.setdefault(session_id, set())
        order = self._session_key_orders.setdefault(session_id, list(self._live_keys))
        remaining = sum(1 for key in order if key in self._live_keys and key not in seen)
        has_more = remaining > 0

        if has_more:
            logger.info(f"KeyRing '{self.pool_name}': {remaining} live keys remain after rotation")
        else:
            logger.critical(
                f"KeyRing '{self.pool_name}': All {self.live_size} live keys exhausted"
                f" for session {session_id}"
            )

        return has_more

    def end_session(self, session_id: int) -> None:
        """Clean up session tracking."""
        self._current_session_attempts.pop(session_id, None)
        self._session_key_orders.pop(session_id, None)
        self._session_seen_keys.pop(session_id, None)

    @property
    def size(self) -> int:
        return len(self.keys)


class ResiliencyExecutor:
    """Execute API requests with key rotation and resiliency termination.

    On quota exhaustion raises :class:`QuotaExhaustedError` so the caller (Maia
    agent layer) can act (typically restarting the container for IP rotation);
    a catchable exception, not ``sys.exit``, so it never kills an unrelated
    host process importing Atlas.
    """

    def __init__(self, key_ring: KeyRing, agent_name: str = "unknown"):
        self.key_ring = key_ring
        self.agent_name = agent_name
        self.logger = logging.getLogger(f"atlas.resiliency.{agent_name}")

    async def execute_async(
        self,
        request_func: Callable[[str], Any],
        error_classifier: Callable[[Exception], tuple[bool, bool]] | None = None,
    ) -> Any | None:
        """Execute an API request with key rotation; raises
        QuotaExhaustedError when all keys are exhausted."""
        session_id = self.key_ring.start_session()

        try:
            while True:
                key = self.key_ring.get_session_key(session_id)

                try:
                    result = await request_func(key)
                    self.logger.debug(f"Request succeeded with key {key[-6:]}")
                    return result

                except Exception as e:
                    disposition = self._classify_error(e, error_classifier)

                    if disposition in ("dead_key", "quota"):
                        if disposition == "dead_key":
                            self.logger.warning(f"Dead API key {key[-6:]}: {e}")
                            self.key_ring.mark_key_dead(key)
                        else:
                            self.logger.warning(f"Quota/rate error with key {key[-6:]}: {e}")

                        if self.key_ring.attempt_rotation(session_id):
                            continue  # Retry with next key
                        # All keys exhausted — raise a catchable
                        # QuotaExhaustedError so the caller decides the resiliency action.
                        self.logger.critical(
                            f"RESILIENCY: All keys exhausted for {self.agent_name}. "
                            f"Signalling quota exhaustion to caller."
                        )
                        raise QuotaExhaustedError(
                            f"All API keys exhausted for {self.agent_name}"
                        ) from e

                    if disposition == "retryable":
                        self.logger.warning(f"Retryable error: {e}")
                        if self.key_ring.attempt_rotation(session_id):
                            continue
                        self.logger.exception("All retry attempts exhausted")
                        return None
                    self.logger.exception(f"Non-retryable error: {e}")
                    raise

        finally:
            self.key_ring.end_session(session_id)

    def _classify_error(
        self,
        exception: Exception,
        error_classifier: Callable[[Exception], tuple[bool, bool]] | None = None,
    ) -> ErrorDisposition:
        """Classify a request failure for rotation and key retirement."""
        if error_classifier:
            is_quota_error, is_retryable = error_classifier(exception)
            if is_quota_error:
                return "quota"
            return "retryable" if is_retryable else "fatal"

        if isinstance(exception, YouTubeAPIError):
            reason = (exception.reason or "").lower()
            if reason in {"keyinvalid", "keyexpired"}:
                return "dead_key"
            if exception.status == 429 or reason in {
                "quotaexceeded",
                "dailylimitexceeded",
                "dailylimitexceededunreg",
                "ratelimitexceeded",
                "ratelimitexceededunreg",
                "userratelimitexceeded",
                "userratelimitexceededunreg",
                "servinglimitexceeded",
                "concurrentlimitexceeded",
                "limitexceeded",
                "variabletermexpireddailyexceeded",
                "variabletermlimitexceeded",
            }:
                return "quota"
            # accessNotConfigured, generic 401/403 auth failures, and all
            # other typed API errors are configuration/resource failures. They
            # must not retire a key or churn through the ring.
            return "fatal"

        # Legacy callers still pass plain exceptions. Preserve their rotation
        # behavior, but never permanently blacklist a key without an explicit
        # structured key-invalid reason.
        return "quota" if self._is_quota_error(exception) else "fatal"

    def _is_quota_error(self, exception: Exception) -> bool:
        """Legacy string fallback for untyped provider exceptions.

        Structured :class:`YouTubeAPIError` instances are classified by their
        explicit reason in :meth:`_classify_error`; these heuristics remain only
        for compatibility with older callers.
        """
        if isinstance(exception, YouTubeAPIError):
            return self._classify_error(exception) == "quota"
        error_str = str(exception).lower()
        status_tokens = ("http 401", "http 403", "http 429", "401", "403", "429")
        if any(token in error_str for token in status_tokens):
            return True
        quota_indicators = [
            "quota",
            "rate limit",
            "quotaexceeded",
            "usagelimit",
        ]
        return any(indicator in error_str for indicator in quota_indicators)
