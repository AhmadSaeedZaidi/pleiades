"""Tests for the Janitor archival vault-safety gates.

Regression for a rediscovered bug: janitor archival deleted transcript rows and
zeroed ``has_*`` flags with **no check** that the content ever reached the vault,
so (a) unflushed transcripts were silently destroyed and (b) the heartbeat's hot
``transcripts``/``with_visuals``/``audios`` counts shrank every cycle.

The fix — assert these in the SQL the repository issues:
  1. ``sweep_archivable`` / ``count_archivable`` exclude any video whose
     transcript isn't confirmed in the vault (unflushable, or ``vault_uri IS NULL``).
  2. ``archive_video_batch`` refuses to purge a video whose transcript still
     lacks a ``vault_uri``.
"""

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from atlas.models import Video
from atlas.repositories.video.janitor import VideoJanitorMixin

from tiered_storage import StagedItem, StoredItem


class FakeJanitor(VideoJanitorMixin):
    """VideoJanitorMixin with the low-level DB helpers stubbed out."""

    def __init__(self) -> None:
        self._fetch_all = AsyncMock()
        self._fetch_one = AsyncMock()
        self._execute = AsyncMock()


def _make_video(video_id: str) -> Video:
    return Video(
        id=video_id,
        channel_id="ch",
        title="t",
        status="PROCESSED",
        last_updated_at=datetime.now(UTC) - timedelta(days=30),
    )


@pytest.mark.asyncio
async def test_sweep_excludes_unvaulted_transcript():
    """A processed video whose transcript was never written must not archive."""
    s = FakeJanitor()
    s._fetch_all.return_value = []
    await s.sweep_archivable(10)
    sql = s._fetch_all.call_args[0][0]
    assert "vault_write_pending IS NOT TRUE" in sql
    assert "t.vault_uri IS NULL" in sql


@pytest.mark.asyncio
async def test_count_archivable_applies_gate():
    s = FakeJanitor()
    s._fetch_one.return_value = {"total": 0}
    await s.count_archivable()
    sql = s._fetch_one.call_args[0][0]
    assert "vault_write_pending IS NOT TRUE" in sql
    assert "t.vault_uri IS NULL" in sql


async def test_archive_guard_is_inside_mutation_transaction():
    """There is no race between a separate safety check and unconditional deletion."""
    janitor = FakeJanitor()
    conn = MagicMock()
    conn.transaction.return_value.__aenter__ = AsyncMock()
    conn.transaction.return_value.__aexit__ = AsyncMock()
    cur = MagicMock()
    cur.fetchall = AsyncMock(return_value=[])
    conn.execute = AsyncMock(return_value=cur)

    @asynccontextmanager
    async def connection():
        yield conn

    janitor._connection = connection
    completed = await janitor._finalize_archives(
        [StoredItem(StagedItem("V1", "123", b"{}"), "memory://metadata")]
    )
    assert completed == set()
    assert conn.execute.await_count == 2
    sql = conn.execute.await_args_list[1].args[0]
    assert "v.xmin::text = staged.version" in sql
    assert "v.vault_write_pending IS NOT TRUE" in sql
    assert "t.vault_uri IS NULL OR t.content IS NOT NULL" in sql
    assert not any(
        "DELETE FROM transcripts" in call.args[0] for call in conn.execute.await_args_list
    )
