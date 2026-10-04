import pytest
from atlas.db import db


@pytest.mark.asyncio
async def test_unit_suite_cannot_open_pipeline_database(isolate_unit_database):
    with pytest.raises(pytest.fail.Exception, match="Unit test attempted PostgreSQL"):
        async with db.get_connection():
            pytest.fail("Database guard did not stop the connection")
