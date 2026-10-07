"""Maia Muralist: full-video extraction agent ("super painter").

Manual-only capability (no systemd unit): downloads a video at compact native
resolution (no re-encode) and archives it to the vault. Intended for future
large-scale video hoarding once storage + compute are available.
"""

from maia.muralist.flow import MuralistAgent, muralist_flow

__all__ = ["MuralistAgent", "muralist_flow"]
