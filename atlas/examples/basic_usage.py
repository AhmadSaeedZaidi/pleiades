"""
Basic usage examples for Atlas infrastructure library.
"""

import asyncio

from atlas import (
    AlertChannel,
    AlertLevel,
    db,
    events,
    get_vault,
    notifier,
    settings,
)


async def database_example() -> None:
    """Demonstrate database operations."""
    print("Database Example:")

    is_healthy = await db.health_check()
    print(f"  Database health: {'OK' if is_healthy else 'FAILED'}")

    async with db.get_connection() as conn:
        result = await conn.execute("SELECT COUNT(*) FROM system_events")
        async for row in result:
            print(f"  Total events: {row[0]}")


async def storage_example() -> None:
    """Demonstrate storage operations."""
    print("\nStorage Example:")
    print(f"  Active vault: {settings.VAULT_PROVIDER}")

    vault = get_vault()

    # Store metadata (date-partitioned)
    metadata = {
        "video_id": "test123",
        "title": "Example Video",
        "tags": ["demo", "atlas"],
        "category_id": "28",
        "views": 1000,
    }
    vault.store_metadata("test123", metadata)
    print("  Stored metadata")

    # Store transcript
    transcript = {"text": "This is an example transcript", "language": "en"}
    vault.store_transcript("test123", transcript)
    print("  Stored transcript")

    # Fetch data
    retrieved_meta = vault.fetch_metadata("test123", "2026-01-09")
    retrieved_trans = vault.fetch_transcript("test123")
    if retrieved_meta:
        print(f"  Retrieved metadata: {retrieved_meta['title']}")
    if retrieved_trans:
        print(f"  Retrieved transcript: {len(retrieved_trans['text'])} chars")


async def events_example() -> None:
    """Demonstrate event emission."""
    print("\nEvents Example:")

    await events.emit(
        event_type="test.event",
        entity_id="example123",
        payload={"action": "demo", "status": "success"},
    )
    print("  Event emitted successfully")


async def notification_example() -> None:
    """Demonstrate notifications."""
    print("\nNotification Example:")

    await notifier.send(
        title="Test Alert",
        description="This is a test notification from Atlas",
        channel=AlertChannel.OPS,
        level=AlertLevel.INFO,
        fields={
            "Environment": settings.ENV,
            "Compliance": str(settings.COMPLIANCE_MODE),
        },
    )
    print("  Notification sent")


async def main() -> None:
    """Run all examples."""
    try:
        print("=" * 60)
        print("Atlas Infrastructure Library - Usage Examples")
        print("=" * 60)

        await database_example()
        await storage_example()
        await events_example()
        await notification_example()

        print("\n" + "=" * 60)
        print("All examples completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\nError: {e}")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
