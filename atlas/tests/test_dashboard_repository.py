from unittest.mock import AsyncMock, MagicMock

import pytest
from atlas.repositories.dashboard import DashboardRepository


def reader():
    repo = DashboardRepository(MagicMock())
    repo.rows = AsyncMock(side_effect=[[{"total": 1}], [{"id": "abcdefghijk"}]])
    return repo


@pytest.mark.asyncio
async def test_search_is_bound_and_pattern_characters_are_literal():
    repo = reader()
    await repo.videos(search="50%_title'", status="PENDING", page=2, page_size=10)
    for call in repo.rows.await_args_list:
        sql, params = call.args
        assert "50%_title'" not in sql
        assert params[0] == "%50\\%\\_title'%"
        assert params[2] == params[0]
    assert repo.rows.await_args_list[1].args[1][-2:] == (10, 10)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        {"stage": "raw; DROP TABLE videos"},
        {"status": "UNKNOWN"},
        {"page": 0},
        {"page_size": 51},
        {"search": "a" * 121},
    ],
)
async def test_repository_rejects_untrusted_identifiers_and_unbounded_reads(kwargs):
    repo = reader()
    with pytest.raises(ValueError):
        await repo.videos(**kwargs)
    repo.rows.assert_not_called()


@pytest.mark.asyncio
async def test_failed_stage_filter_and_stable_ordering():
    repo = reader()
    await repo.videos(stage="audio")
    sql = repo.rows.await_args_list[1].args[0]
    assert "v.audio_phase = 'FAILED'" in sql
    assert "ORDER BY v.discovered_at DESC, v.id DESC" in sql


@pytest.mark.asyncio
async def test_events_exclude_payloads_and_are_bounded():
    repo = reader()
    repo.rows = AsyncMock(return_value=[])
    await repo.events()
    sql = repo.rows.await_args.args[0]
    assert "payload" not in sql
    assert "LIMIT 80" in sql


@pytest.mark.asyncio
async def test_missing_video_does_not_query_stats():
    repo = reader()
    repo.rows = AsyncMock(return_value=[])
    assert await repo.video("abcdefghijk") is None
    repo.rows.assert_awaited_once()


@pytest.mark.asyncio
async def test_detail_caps_transcript_and_stats():
    repo = reader()
    repo.rows = AsyncMock(side_effect=[[{"id": "abcdefghijk"}], [{"views": 2}, {"views": 1}]])
    result = await repo.video("abcdefghijk")
    assert result["stats"] == [{"views": 1}, {"views": 2}]
    assert "LEFT(t.content::text, 24000)" in repo.rows.await_args_list[0].args[0]
    assert "LIMIT 30" in repo.rows.await_args_list[1].args[0]
