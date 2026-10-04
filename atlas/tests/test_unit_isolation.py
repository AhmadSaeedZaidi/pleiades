import socket

import pytest
from atlas.db import db

from atlas import state


@pytest.mark.asyncio
async def test_unit_suite_cannot_open_pipeline_database(isolate_unit_database):
    with pytest.raises(pytest.fail.Exception, match="Unit test attempted PostgreSQL"):
        async with db.get_connection():
            pytest.fail("Database guard did not stop the connection")


def test_unit_suite_cannot_connect_to_external_services():
    with socket.socket() as connection:
        with pytest.raises(pytest.fail.Exception, match="Unit test attempted network"):
            connection.connect(("127.0.0.1", 9))
        with pytest.raises(pytest.fail.Exception, match="Unit test attempted network"):
            connection.connect_ex(("127.0.0.1", 9))
    with pytest.raises(pytest.fail.Exception, match="Unit test attempted network"):
        socket.getaddrinfo("example.com", 443)


def test_unit_state_is_private_to_the_test(tmp_path):
    assert state._STATE_PATH == tmp_path / "agent_state.json"
    assert state._read() == {}
    state.mark_quota_exhausted("isolated-test")
    assert state.quota_exhausted_agents() == ["isolated-test"]
    assert state._STATE_PATH.exists()
