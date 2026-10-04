"""Read-only preflight checks for explicitly authorized live services."""

from collections.abc import AsyncIterator

import pytest

from atlas import db, settings


@pytest.fixture(autouse=True)
async def close_db_between_tests() -> AsyncIterator[None]:
    """Keep the live DB pool tied to the current pytest event loop."""
    await db.close()
    yield
    await db.close()


@pytest.mark.live
@pytest.mark.smoke
@pytest.mark.asyncio
async def test_database_health() -> None:
    """Run the Atlas SELECT 1 health check without changing database state."""
    await db.initialize()
    assert await db.health_check(), "Database health check failed"


@pytest.mark.live
@pytest.mark.smoke
def test_vault_repository_exists() -> None:
    """Read the configured HF repository or GCS bucket metadata."""
    if settings.VAULT_PROVIDER == "huggingface":
        from huggingface_hub import HfApi

        assert settings.HF_DATASET_ID, "HF_DATASET_ID is not configured"
        assert settings.HF_TOKEN, "HF_TOKEN is not configured"
        info = HfApi(token=settings.HF_TOKEN.get_secret_value()).repo_info(
            repo_id=settings.HF_DATASET_ID,
            repo_type="dataset",
        )
        assert info is not None, "Configured Hugging Face dataset was not found"
        return

    if settings.VAULT_PROVIDER == "gcs":
        from google.cloud import storage

        assert settings.GCS_BUCKET_NAME, "GCS_BUCKET_NAME is not configured"
        client = storage.Client()
        bucket = client.bucket(settings.GCS_BUCKET_NAME)
        assert bucket.exists(client=client), "Configured GCS bucket was not found"


@pytest.mark.live
@pytest.mark.smoke
def test_required_configuration_and_api_keys() -> None:
    """Validate required settings locally without making a YouTube API call."""
    assert settings.DATABASE_URL, "DATABASE_URL is not configured"
    assert settings.ENV.strip(), "ENV is not configured"
    assert isinstance(settings.COMPLIANCE_MODE, bool), "COMPLIANCE_MODE must be boolean"

    keys = settings.api_keys
    assert keys, "YOUTUBE_API_KEY_POOL_JSON contains no keys"
    dummy_markers = ("DUMMY", "TEST_", "FAKE", "MOCK", "EXAMPLE")
    assert all(
        len(key) >= 30 and not any(marker in key.upper() for marker in dummy_markers)
        for key in keys
    ), "YOUTUBE_API_KEY_POOL_JSON contains a missing or dummy key"
