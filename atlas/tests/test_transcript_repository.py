"""Transcript handoffs use one conditional batch transaction."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest
from atlas.repositories.video.transcript import TranscriptRepository

from tiered_storage import StagedItem, StoredItem


def repository(returned):
    repo = TranscriptRepository(db_pool=MagicMock())
    conn = MagicMock()
    conn.transaction.return_value.__aenter__ = AsyncMock()
    conn.transaction.return_value.__aexit__ = AsyncMock()
    cur = MagicMock()
    cur.fetchall = AsyncMock(return_value=returned)
    conn.execute = AsyncMock(return_value=cur)

    @asynccontextmanager
    async def connection():
        yield conn

    repo._connection = connection
    return repo, conn


async def test_finalizes_many_transcripts_and_video_markers_together():
    repo, conn = repository([("V1",), ("V2",)])
    items = [StoredItem(StagedItem(key, "123", b"{}"), f"memory://{key}") for key in ("V1", "V2")]
    assert await repo.finalize(items) == {"V1", "V2"}
    assert conn.execute.await_count == 2
    lock_sql = conn.execute.await_args_list[0].args[0]
    sql, params = conn.execute.await_args_list[1].args
    assert "ORDER BY id FOR UPDATE" in lock_sql
    assert "t.xmin::text = s.version" in sql
    assert "content = NULL" in sql and "UPDATE videos AS v" in sql
    assert params[:3] == (["V1", "V2"], ["123", "123"], ["memory://V1", "memory://V2"])


async def test_missing_uri_never_enters_finalization_transaction():
    repo, conn = repository([])
    with pytest.raises(ValueError, match="vault_uri is required"):
        await repo.finalize([StoredItem(StagedItem("V1", "123", b"{}"), "")])
    conn.execute.assert_not_awaited()


async def test_selection_serializes_payload_and_captures_version():
    repo, _ = repository([])
    repo._fetch_all = AsyncMock(
        return_value=[
            {"id": "V1", "version": "123", "existing_uri": None, "transcript": [{"text": "hello"}]}
        ]
    )
    items = await repo.pending(50)
    assert items[0].payload == b'[{"text":"hello"}]'
    assert items[0].version == "123"
    assert items[0].path.startswith("transcripts/V1/V1/")
    assert repo._fetch_all.await_args.args[1] == (50, 50, 50, 50)
