"""Process-wide client-side pacing for paid STT providers (Grok/Mistral).

Paces our own calls so we stay under provider per-minute quotas rather than
relying on their 429 alone. Thread-safe — used inside ``asyncio.to_thread`` workers.
"""

import threading
import time


class CallPacer:
    """Ensures a minimum spacing between successive calls, process-wide."""

    def __init__(self, min_interval: float) -> None:
        self.min_interval = min_interval
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self.min_interval - (now - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
