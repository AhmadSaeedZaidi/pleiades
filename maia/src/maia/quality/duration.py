"""ISO-8601 duration parsing + HTTP status helpers for the quality gate."""

import re

# ISO-8601 durations as returned by the YouTube API, e.g. ``PT1M5S``, ``PT1H2M3S``.
_DURATION_RE = re.compile(
    r"^P(?:(?P<days>\d+)D)?T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?$"
)

# Redirect status codes returned when a Shorts URL points at a long-form video.
_REDIRECT_STATUS = frozenset({301, 302, 303, 307, 308})


def parse_iso8601_duration(value: str | None) -> int:
    """Parse an ISO-8601 duration (``PT1M5S``) into whole seconds.

    Returns 0 for empty/unparseable input or zero-length durations (e.g. live
    streams reported as ``P0D``), which the duration gate then rejects.
    """
    if not value:
        return 0
    m = _DURATION_RE.match(value)
    if not m:
        return 0
    days = int(m.group("days") or 0)
    hours = int(m.group("hours") or 0)
    minutes = int(m.group("minutes") or 0)
    seconds = int(m.group("seconds") or 0)
    return days * 86400 + hours * 3600 + minutes * 60 + seconds
