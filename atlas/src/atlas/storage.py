"""Pleiades vault adapter for the independent tiered_storage package."""

import io
import json
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from atlas.vault import VaultStrategy
from atlas.vault_io import run_vault_io
from tiered_storage import StagedItem


class VaultColdStore:
    def __init__(
        self,
        vault: VaultStrategy,
        *,
        heads: dict[str, str] | None = None,
        legacy_json_uris: set[str] | None = None,
        run: Callable[[Callable[[], Any]], Awaitable[Any]] = run_vault_io,
    ) -> None:
        self.vault, self.heads, self.run = vault, heads or {}, run
        self.legacy_json_uris = legacy_json_uris or set()

    async def write_batch(self, items: Sequence[StagedItem]) -> dict[str, str]:
        # Immutable bodies and tiny compatibility pointers share one HF commit.
        # On GCS bodies precede pointers; incomplete writes never finalize hot data.
        bodies: dict[str, Any] = {item.path: io.BytesIO(item.payload) for item in items}
        pointers = [
            (self.heads[item.key], {"_tiered_storage": 1, "path": item.path, "sha256": item.sha256})
            for item in items
            if item.key in self.heads
        ]
        paths = list(bodies)
        uris = await self.run(lambda: self.vault.store_batch(list(bodies.items()) + pointers))
        if len(uris) != len(paths) + len(pointers) or any(
            not isinstance(uri, str) or not uri.endswith("/" + path)
            for path, uri in zip(paths, uris[: len(paths)], strict=True)
        ):
            raise ValueError("Vault returned incomplete or mismatched object URIs")
        by_path = dict(zip(paths, uris[: len(paths)], strict=True))
        return {item.key: by_path[item.path] for item in items}

    async def read(self, uri: str) -> bytes | None:
        def fetch() -> bytes | None:
            buffer = self.vault.fetch_binary(uri)
            if buffer is None:
                return None
            try:
                payload = buffer.read()
                # Legacy JSON encodings differ in whitespace/key order. Verify
                # their logical content against the canonical staged payload.
                if uri in self.legacy_json_uris:
                    payload = json.dumps(
                        json.loads(payload),
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                        allow_nan=False,
                    ).encode("utf-8")
                return payload
            finally:
                buffer.close()

        payload: bytes | None = await self.run(fetch)
        return payload
