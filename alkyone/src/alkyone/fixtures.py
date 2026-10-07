import os

# Must run before any atlas import: atlas settings snapshot HF_DATASET_ID at import time.
_TEST_VAULT_DEFAULT = "Rolaficus/pleiades-vault-test"
_USE_PROD_VAULT = os.getenv("PLEIADES_USE_PRODUCTION_VAULT", "").lower() in (
    "1",
    "true",
    "yes",
)
if _USE_PROD_VAULT:
    # PLEIADES_USE_PRODUCTION_VAULT makes the fixtures stop overriding the vault
    # and lets integration tests write to (and, in _cleanup_hf_uploads, DELETE
    # from) the production HuggingFace dataset. It is a deliberate, documented
    # escape hatch, so it additionally refuses to run alongside the integration
    # suite unless the live opt-in is also present. Previously, setting this plus
    # ALKYONE_TEST_VAULT=<the prod vault> satisfied every guard check and the
    # suite then deleted files from production.
    if os.getenv("ALKYONE_ALLOW_LIVE", "").strip().lower() not in {"1", "true", "yes", "on"}:
        raise SystemExit(
            "\nALKYONE REFUSAL: PLEIADES_USE_PRODUCTION_VAULT is set, which points the "
            "test fixtures at the production vault.\n"
            "It also requires ALKYONE_ALLOW_LIVE=1, because the suite can delete files "
            "from the production dataset during cleanup.\n"
            "Unset PLEIADES_USE_PRODUCTION_VAULT to run isolated integration tests.\n"
        )
else:
    os.environ["HF_DATASET_ID"] = os.getenv("HF_DATASET_ID_TEST", _TEST_VAULT_DEFAULT)

import logging  # noqa: E402
from collections.abc import AsyncGenerator  # noqa: E402
from typing import Any  # noqa: E402

import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402
from atlas.config import settings  # noqa: E402

from atlas import db  # noqa: E402

logger = logging.getLogger("alkyone.fixtures")
logger.info(f"Test session vault: {settings.HF_DATASET_ID}")


def _mirror_settings_to_environ() -> None:
    """Mirror loaded ``atlas.config.settings`` values back into ``os.environ``."""
    pairs: list[tuple[str, str]] = []

    if settings.DATABASE_URL and not os.getenv("DATABASE_URL"):
        pairs.append(("DATABASE_URL", str(settings.DATABASE_URL)))

    if settings.HF_TOKEN and not os.getenv("HF_TOKEN"):
        pairs.append(("HF_TOKEN", settings.HF_TOKEN.get_secret_value()))

    if settings.HF_DATASET_ID and not os.getenv("HF_DATASET_ID"):
        pairs.append(("HF_DATASET_ID", settings.HF_DATASET_ID))

    if settings.VAULT_PROVIDER and not os.getenv("VAULT_PROVIDER"):
        pairs.append(("VAULT_PROVIDER", settings.VAULT_PROVIDER))

    if not os.getenv("YOUTUBE_API_KEY_POOL_JSON"):
        try:
            raw = settings.YOUTUBE_API_KEY_POOL_JSON.get_secret_value()
            pairs.append(("YOUTUBE_API_KEY_POOL_JSON", raw))
        except Exception:
            pass

    if settings.youtube_cookies_resolved_path and not os.getenv("YOUTUBE_COOKIES_PATH"):
        pairs.append(("YOUTUBE_COOKIES_PATH", settings.youtube_cookies_resolved_path))

    if settings.PREFECT_API_URL and not os.getenv("PREFECT_API_URL"):
        pairs.append(("PREFECT_API_URL", settings.PREFECT_API_URL))

    if settings.PREFECT_API_KEY and not os.getenv("PREFECT_API_KEY"):
        pairs.append(("PREFECT_API_KEY", settings.PREFECT_API_KEY.get_secret_value()))

    for k, v in pairs:
        os.environ[k] = v


_mirror_settings_to_environ()

os.environ["ENV"] = os.getenv("ENV", "development")
os.environ["COMPLIANCE_MODE"] = os.getenv("COMPLIANCE_MODE", "False")

if not os.getenv("YOUTUBE_API_KEY_POOL_JSON"):
    logger.warning(
        "YOUTUBE_API_KEY_POOL_JSON not set! Using fallback dummy key. "
        "Integration tests requiring real YouTube API will be skipped."
    )
    os.environ["YOUTUBE_API_KEY_POOL_JSON"] = '["DUMMY_KEY_FOR_UNIT_TESTS_ONLY"]'

_uploaded_files: list[str] = []


@pytest_asyncio.fixture(scope="session")
async def system_init() -> AsyncGenerator[None, None]:
    """Session-level setup that validates HuggingFace credentials for integration tests."""
    # Prod-safety interlock (belt-and-suspenders with the conftest guard): never
    # run integration tests against production infrastructure.
    from alkyone.guard import assert_not_production

    assert_not_production()

    logger.info("Alkyone: Initializing System for Testing...")

    if not os.getenv("HF_TOKEN"):
        logger.warning(
            "HF_TOKEN not set! Integration tests requiring vault will fail. "
            "Set it with: export HF_TOKEN='hf_xxxxx'"
        )

    if not os.getenv("HF_DATASET_ID"):
        logger.warning(
            "HF_DATASET_ID not set! Using fallback or tests will fail. "
            "Set it with: export HF_DATASET_ID='username/pleiades-test-vault'"
        )

    yield
    await _cleanup_hf_uploads()
    logger.info("Alkyone: System Teardown Complete.")


@pytest_asyncio.fixture(scope="function")
async def fresh_db(system_init: Any) -> AsyncGenerator[None, None]:
    """Function-level fixture that provides a clean database for each test."""
    if db._pool is not None:
        await db.close()
        db._pool = None

    await db.initialize()

    try:
        await db.setup_test_schema()
        await db.reset_for_test()

        yield

    finally:
        await db.close()
        db._pool = None


def track_hf_upload(path: str) -> None:
    """Track a file uploaded to HuggingFace for cleanup."""
    global _uploaded_files
    if path not in _uploaded_files:
        _uploaded_files.append(path)
        logger.debug(f"Tracked HF upload: {path}")


async def _cleanup_hf_uploads() -> None:
    """Clean up all files uploaded to HuggingFace during tests."""
    global _uploaded_files

    if not _uploaded_files:
        logger.info("No HuggingFace uploads to clean up.")
        return

    logger.info(f"Cleaning up {len(_uploaded_files)} files from HuggingFace...")

    try:
        from huggingface_hub import HfApi

        token = os.getenv("HF_TOKEN")
        repo_id = os.getenv("HF_DATASET_ID")

        if not token or not repo_id:
            logger.warning("Cannot cleanup HF uploads: HF_TOKEN or HF_DATASET_ID not set")
            return

        api = HfApi(token=token)

        for file_path in _uploaded_files:
            try:
                api.delete_file(
                    path_in_repo=file_path,
                    repo_id=repo_id,
                    repo_type="dataset",
                    commit_message=f"Test cleanup: Remove {file_path}",
                )
                logger.debug(f"Deleted HF file: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to delete {file_path}: {e}")

        logger.info("HuggingFace cleanup complete.")
        _uploaded_files.clear()

    except ImportError:
        logger.warning("huggingface_hub not installed, skipping cleanup")
    except Exception as e:
        logger.error(f"Error during HuggingFace cleanup: {e}")


@pytest.fixture
def mock_search_queue_item() -> dict[str, Any]:
    """Mock search queue item for Hunter tests."""
    return {
        "id": 1,
        "query_term": "test query",
        "next_page_token": None,
        "last_searched_at": None,
        "priority": 5,
    }


@pytest.fixture
def mock_youtube_search_response() -> dict[str, Any]:
    """Mock YouTube search API response."""
    return {
        "items": [
            {
                "id": {"videoId": "TEST123"},
                "snippet": {
                    "title": "Test Video",
                    "channelId": "CHANNEL123",
                    "channelTitle": "Test Channel",
                    "publishedAt": "2026-01-15T10:00:00Z",
                    "description": "Test description",
                    "tags": ["test", "video"],
                },
            }
        ],
        "nextPageToken": "NEXT_TOKEN",
    }


@pytest.fixture
def mock_tracker_target() -> dict[str, Any]:
    """Mock tracker target video."""
    return {
        "id": "TEST123",
        "title": "Test Video",
        "published_at": "2026-01-15T10:00:00Z",
        "last_updated_at": None,
    }


@pytest.fixture
def mock_youtube_stats_response() -> dict[str, Any]:
    """Mock YouTube statistics API response."""
    return {
        "items": [
            {
                "id": "TEST123",
                "snippet": {"publishedAt": "2026-01-15T10:00:00Z"},
                "statistics": {
                    "viewCount": "1000",
                    "likeCount": "100",
                    "commentCount": "10",
                },
            }
        ]
    }
