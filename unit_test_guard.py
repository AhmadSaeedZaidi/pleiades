"""Keep unit tests away from live databases, network services and agent state."""

import importlib.util
import sys
from contextlib import ExitStack, asynccontextmanager
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def isolate_unit_database(request, tmp_path):
    # Alkyone owns its integration/live guards. Never weaken or substitute them.
    path = str(request.node.path)
    if any(f"/alkyone/tests/{tier}/" in path for tier in ("components", "live", "storage")):
        yield
        return

    async def refuse(*args, **kwargs):
        pytest.fail("Unit test attempted PostgreSQL access; mock its repository or event bus")

    def refuse_sync(*args, **kwargs):
        pytest.fail("Unit test attempted PostgreSQL access; mock its repository or event bus")

    def refuse_network(*args, **kwargs):
        pytest.fail("Unit test attempted network access; mock its external collaborator")

    @asynccontextmanager
    async def refuse_repository(*args, **kwargs):
        pytest.fail("Unit test attempted PostgreSQL access; mock its repository or event bus")
        yield  # pragma: no cover - this is an async context manager that always fails

    with ExitStack() as stack:
        # Tests may exercise real state logic, but each owns a disposable file.
        if importlib.util.find_spec("atlas") is not None:
            stack.enter_context(patch("atlas.state._STATE_PATH", tmp_path / "agent_state.json"))
        stack.enter_context(patch("socket.getaddrinfo", side_effect=refuse_network))
        stack.enter_context(patch("socket.socket.connect", side_effect=refuse_network))
        stack.enter_context(patch("socket.socket.connect_ex", side_effect=refuse_network))
        stack.enter_context(patch("psycopg.AsyncConnection.connect", side_effect=refuse))
        stack.enter_context(patch("psycopg.Connection.connect", side_effect=refuse_sync))
        if "atlas.db" in sys.modules:
            manager = sys.modules["atlas.db"].DatabaseManager
            stack.enter_context(patch.object(manager, "get_connection", refuse_repository))
        yield
