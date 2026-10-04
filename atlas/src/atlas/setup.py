import asyncio
import logging

from atlas.adapters import DatabaseAdapter

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("atlas.setup")


class SchemaManager(DatabaseAdapter):
    async def provision(self) -> None:
        logger.info("Provisioning database schema...")
        from atlas.db import db

        try:
            await db.provision_schema()
            logger.info("Schema provisioned successfully")
        except Exception as e:
            logger.exception(f"Provisioning failed: {e}")
            raise


async def provision_schema() -> None:
    manager = SchemaManager()
    try:
        await manager.provision()
    finally:
        from atlas.db import db

        await db.close()


if __name__ == "__main__":
    asyncio.run(provision_schema())
