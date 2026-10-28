"""Agent protocol for polymorphic command dispatch.

Every Maia agent must satisfy this structural contract so it can be
registered in :data:`maia.registry.AGENT_REGISTRY` and dispatched via
the CLI entry-point.
"""

import argparse
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Agent(Protocol):
    """Structural contract for Maia agents."""

    name: str

    @staticmethod
    def add_cli_args(parser: argparse.ArgumentParser) -> None: ...

    async def run(self, **kwargs: Any) -> dict[str, Any]: ...
