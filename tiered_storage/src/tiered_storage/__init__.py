"""Backend-independent, verified hot/cold storage. No application imports."""

from .core import (
    ColdStore,
    HotObject,
    HotStore,
    StagedItem,
    StoredItem,
    TieredStorage,
    TransferFailure,
    TransferPolicy,
    TransferResult,
    promote,
)
from .local import FilesystemColdStore, SQLiteHotStore

__all__ = [
    "ColdStore",
    "FilesystemColdStore",
    "HotObject",
    "HotStore",
    "SQLiteHotStore",
    "StagedItem",
    "StoredItem",
    "TieredStorage",
    "TransferFailure",
    "TransferPolicy",
    "TransferResult",
    "promote",
]
