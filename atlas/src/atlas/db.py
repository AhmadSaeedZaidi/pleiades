import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import sqlparse
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from atlas.config import settings

logger = logging.getLogger("atlas.db")


class DatabaseManager:
    _instance: Optional["DatabaseManager"] = None
    _pool: AsyncConnectionPool | None = None

    def __new__(cls) -> "DatabaseManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if not hasattr(self, "dsn"):
            self.dsn = str(settings.DATABASE_URL)

    async def initialize(self) -> None:
        if self._pool is None:
            logger.info("Atlas: Connecting to Database...")
            self._pool = AsyncConnectionPool(
                self.dsn,
                min_size=0,
                max_size=20,
                timeout=30.0,
                open=True,
            )

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("Atlas: DB Connection Closed.")

    async def health_check(self) -> bool:
        try:
            async with self.get_connection() as conn:
                await conn.execute("SELECT 1")
            return True
        except Exception as e:
            logger.exception(f"Health check failed: {e}")
            return False

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[AsyncConnection, None]:
        if self._pool is None:
            await self.initialize()
        assert self._pool is not None, "Pool initialization failed"
        async with self._pool.connection() as conn:
            yield conn

    async def provision_schema(self) -> None:
        """Provision the production database schema.

        Resilient to managed Postgres (Neon/RDS/Crunchy) without TimescaleDB/
        vector: extension creation and create_hypertable are best-effort while
        CREATE TABLE statements remain mandatory.
        """
        await self._apply_schema(include_extensions=False)

    async def setup_test_schema(self) -> None:
        """Initialize database schema for tests (TimescaleDB/vector best-effort)."""
        await self._apply_schema(include_extensions=False)

    async def _apply_schema(self, *, include_extensions: bool) -> None:
        from psycopg.errors import UndefinedFunction

        timescale_available = True
        # Each extension is attempted in its own connection/transaction so a
        # managed Postgres without the package can't poison a shared transaction.
        for ext_sql in (
            "CREATE EXTENSION IF NOT EXISTS vector;",
            "CREATE EXTENSION IF NOT EXISTS timescaledb;",
        ):
            try:
                async with self.get_connection() as conn:
                    await conn.execute(ext_sql)
            except Exception as exc:  # noqa: BLE001 - best-effort extension setup
                logger.warning(f"Skipping extension ({type(exc).__name__}): {ext_sql}")
                if "timescaledb" in ext_sql:
                    timescale_available = False

        sql = load_schema_sql(include_extensions=include_extensions)
        statements = [s.strip() for s in sqlparse.split(sql) if s.strip()]
        async with self.get_connection() as conn:
            for stmt in statements:
                is_hypertable = "create_hypertable" in stmt.lower()
                if is_hypertable and not timescale_available:
                    logger.warning("Skipping hypertable conversion (TimescaleDB unavailable)")
                    continue
                try:
                    await conn.execute(stmt + ";")
                except UndefinedFunction as exc:
                    if is_hypertable:
                        logger.warning(f"Skipping hypertable conversion: {exc}")
                        continue
                    raise
        logger.info("Schema initialized successfully")

    async def reset_for_test(self) -> None:
        """
        Reset database state for testing.
        """
        async with self.get_connection() as conn:
            await conn.execute(
                """
                DO $$
                DECLARE
                    r RECORD;
                BEGIN
                    -- Iterate over all tables in public schema
                    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                        -- TRUNCATE is faster than DELETE
                        -- RESTART IDENTITY resets serials (id=1)
                        -- CASCADE handles foreign keys
                        EXECUTE (
                            $$TRUNCATE TABLE public.$$ || quote_ident(r.tablename)
                            || $$ RESTART IDENTITY CASCADE$$
                        );
                    END LOOP;
                END $$;
            """
            )
            logger.debug("Database reset for test")


def load_schema_sql(*, include_extensions: bool = True) -> str:
    """Load schema.sql content, optionally excluding CREATE EXTENSION statements.

    Args:
        include_extensions: If False, filter out ``CREATE EXTENSION`` lines.
            Useful for test environments where extensions are created separately.

    Returns:
        The SQL script as a string.
    """
    schema_path = Path(__file__).parent / "schema.sql"
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    sql = schema_path.read_text()

    if not include_extensions:
        filtered_lines = [
            line
            for line in sql.split("\n")
            if not line.strip().upper().startswith("CREATE EXTENSION")
        ]
        sql = "\n".join(filtered_lines)

    return sql


db = DatabaseManager()
