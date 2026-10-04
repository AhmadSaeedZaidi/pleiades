"""Maia agent operations. Metadata imports do not initialize the fleet."""

from importlib import import_module
from typing import Any

__version__ = "0.1.0"
__author__ = "Ahmad Saeed Zaidi"
__license__ = "MIT"

# Preserve public convenience imports without loading every agent on import maia.
_EXPORTS = {
    "GrapherAgent": "grapher",
    "HunterAgent": "hunter",
    "TrackerAgent": "tracker",
    "JanitorAgent": "janitor",
    "ArcheologistAgent": "archeologist",
    "ScribeAgent": "scribe",
    "PainterAgent": "painter",
    "run_hunter_cycle": "hunter",
    "run_tracker_cycle": "tracker",
    "run_archeology_campaign": "archeologist",
    "run_scribe_cycle": "scribe",
    "run_painter_cycle": "painter",
}
__all__ = [*_EXPORTS, "__version__"]


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(f"maia.{_EXPORTS[name]}.flow"), name)
    globals()[name] = value
    return value
