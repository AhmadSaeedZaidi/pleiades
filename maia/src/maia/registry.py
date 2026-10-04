"""
Agent Registry for dynamic command dispatch.

This module provides a centralized registry of all available Maia agents,
enabling polymorphic command dispatch through the main entry point.
"""

from maia.agent import Agent
from maia.archeologist.flow import ArcheologistAgent
from maia.heartbeat.flow import HeartbeatAgent
from maia.hunter.flow import HunterAgent
from maia.janitor.flow import JanitorAgent
from maia.muralist.flow import MuralistAgent
from maia.painter.flow import PainterAgent
from maia.scribe.flow import ScribeAgent
from maia.singer.flow import SingerAgent
from maia.streamer.flow import StreamerAgent
from maia.tracker.flow import TrackerAgent

AGENT_REGISTRY: dict[str, type[Agent]] = {
    "hunter": HunterAgent,
    "tracker": TrackerAgent,
    "janitor": JanitorAgent,
    "archeologist": ArcheologistAgent,
    "scribe": ScribeAgent,
    "painter": PainterAgent,
    "streamer": StreamerAgent,
    "singer": SingerAgent,
    "heartbeat": HeartbeatAgent,
    # Muralist is intentionally NOT fleet-scheduled (no systemd unit); it is a
    # runnable, proven video-extraction capability invoked manually.
    "muralist": MuralistAgent,
}
