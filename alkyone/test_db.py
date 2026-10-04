"""Quick connectivity test using the repository pattern."""

import asyncio

from atlas.repositories import VideoRepository


async def test():
    repo = VideoRepository()
    async with repo._connection() as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, val TEXT)"
        )
        await conn.execute("INSERT INTO test_table (val) VALUES ('hello')")

    async with repo._connection() as conn:
        cur = await conn.execute("SELECT * FROM test_table")
        print(await cur.fetchall())


if __name__ == "__main__":
    asyncio.run(test())
