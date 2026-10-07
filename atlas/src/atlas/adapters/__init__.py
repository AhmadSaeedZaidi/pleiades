import logging
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, Protocol

from psycopg import AsyncConnection

logger = logging.getLogger("atlas.adapters")


class ConnectionProvider(Protocol):
    @asynccontextmanager  # type: ignore[arg-type]
    async def get_connection(self) -> AsyncIterator[AsyncConnection]: ...


class DatabaseAdapterProtocol(Protocol):
    """Structural type modelling the low-level helpers of :class:`DatabaseAdapter`.

    Used by higher-level repository protocols so mixins can be type-checked
    (``mypy --strict``) against the full composed surface without importing the
    concrete adapter.
    """

    def _connection(self) -> AbstractAsyncContextManager[AsyncConnection]: ...

    def _cursor(self) -> AbstractAsyncContextManager[Any]: ...

    async def _execute(self, query: str, params: tuple[Any, ...] | None = None) -> None: ...

    async def _fetch_one(
        self, query: str, params: tuple[Any, ...] | None = None
    ) -> dict[str, Any] | None: ...

    async def _fetch_all(
        self, query: str, params: tuple[Any, ...] | None = None
    ) -> list[dict[str, Any]]: ...

    async def _fetch_many(
        self, query: str, params: tuple[Any, ...] | None, limit: int
    ) -> list[dict[str, Any]]: ...

    async def _execute_many(self, query: str, params_list: list[tuple[Any, ...]]) -> None: ...

    async def _fetch_scalar(
        self, query: str, params: tuple[Any, ...] | None = None
    ) -> Any | None: ...


class DatabaseAdapter:
    def __init__(self, db_pool: ConnectionProvider | None = None) -> None:
        if db_pool is None:
            from atlas.db import db  # type: ignore[assignment]

            db_pool = db  # type: ignore[assignment]
        self._db = db_pool

    @asynccontextmanager
    async def _connection(self) -> AsyncIterator[AsyncConnection]:
        assert self._db is not None
        async with self._db.get_connection() as conn:  # type: ignore[var-annotated]
            yield conn

    @asynccontextmanager
    async def _cursor(self) -> AsyncIterator[Any]:
        assert self._db is not None
        async with self._db.get_connection() as conn, conn.cursor() as cur:  # type: ignore[var-annotated]
            yield cur

    async def _execute(self, query: str, params: tuple[Any, ...] | None = None) -> None:
        async with self._connection() as conn:
            await conn.execute(query, params or ())
            await conn.commit()

    async def _fetch_one(
        self, query: str, params: tuple[Any, ...] | None = None
    ) -> dict[str, Any] | None:
        async with self._cursor() as cur:
            await cur.execute(query, params or ())
            row = await cur.fetchone()
            if not row:
                return None
            columns = [desc[0] for desc in cur.description] if cur.description else []
            return dict(zip(columns, row, strict=True))

    async def _fetch_all(
        self, query: str, params: tuple[Any, ...] | None = None
    ) -> list[dict[str, Any]]:
        async with self._cursor() as cur:
            await cur.execute(query, params or ())
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = await cur.fetchall()
            return [dict(zip(columns, row, strict=True)) for row in rows]

    async def _fetch_many(
        self, query: str, params: tuple[Any, ...] | None, limit: int
    ) -> list[dict[str, Any]]:
        async with self._cursor() as cur:
            await cur.execute(query, params or ())
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = await cur.fetchmany(limit)
            return [dict(zip(columns, row, strict=True)) for row in rows]

    async def _execute_many(self, query: str, params_list: list[tuple[Any, ...]]) -> None:
        async with self._cursor() as cur:
            await cur.executemany(query, params_list)
            await cur.connection.commit()

    async def _fetch_scalar(self, query: str, params: tuple[Any, ...] | None = None) -> Any | None:
        async with self._cursor() as cur:
            await cur.execute(query, params or ())
            row = await cur.fetchone()
            return row[0] if row else None
