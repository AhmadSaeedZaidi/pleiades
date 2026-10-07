"""Set required Atlas env vars before any pleiades_mcp import (collection time)."""

import os

from unit_test_guard import isolate_unit_database as isolate_unit_database

os.environ.update(
    {
        "DATABASE_URL": "postgresql://test:test@127.0.0.1:1/test",
        "YOUTUBE_API_KEY_POOL_JSON": '["k1"]',
        "VAULT_PROVIDER": "huggingface",
        "HF_DATASET_ID": "mock/ds",
        "HF_TOKEN": "mock",
    }
)
