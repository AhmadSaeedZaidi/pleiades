#!/usr/bin/env python3
"""Live MCP-over-HTTP demo for the Pleiades MCP server.

Boots the server, then acts as an MCP client over Streamable HTTP: initializes,
lists tools, and calls a safe read-only tool (`get_video_metadata`) on a known
video id. This proves the endpoint is genuinely usable, not just listening.

Run with:
    .venv/bin/python mcp/demo_live.py
"""

import os
import sys
import time
import json
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
sys.path.insert(0, os.path.abspath("atlas/src"))
sys.path.insert(0, os.path.abspath("maia/src"))

import uvicorn
import pleiades_mcp.server as srv

from mcp.client.streamable_http import streamablehttp_client
from mcp.client.session import ClientSession


def _boot() -> uvicorn.Server:
    srv._http_base = "http://127.0.0.1:8000"
    server = uvicorn.Server(
        uvicorn.Config(
            srv.mcp.streamable_http_app(),
            host="127.0.0.1",
            port=8000,
            log_level="warning",
        )
    )
    threading.Thread(target=server.run, daemon=True).start()
    # wait for bind
    deadline = time.time() + 15
    while time.time() < deadline and not server.started:
        time.sleep(0.1)
    return server


async def _run_client() -> None:
    url = "http://127.0.0.1:8000/mcp"
    async with streamablehttp_client(url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            print("=== Tools exposed by the endpoint ===")
            for t in tools.tools:
                print(f"  - {t.name}: {t.description.splitlines()[0]}")

            print("\n=== Calling get_video_metadata('dQw4w9WgXcQ') ===")
            res = await session.call_tool(
                "get_video_metadata", {"video_id_or_url": "dQw4w9WgXcQ"}
            )
            text = res.content[0].text if res.content else "{}"
            print("  raw response:")
            print("   ", text.replace("\n", "\n    "))
            try:
                data = json.loads(text)
                if "error" in data:
                    print("  (returned error, likely no network/key):", data["error"])
                else:
                    print(f"  title       : {data.get('title')}")
                    print(f"  channel     : {data.get('channel_title')}")
                    print(f"  published_at: {data.get('published_at')}")
                    print(f"  views       : {data.get('statistics', {}).get('views')}")
            except json.JSONDecodeError:
                print("  (non-JSON response)")


def main() -> None:
    server = _boot()
    import asyncio

    try:
        asyncio.run(_run_client())
    finally:
        server.should_exit = True
        time.sleep(1)


if __name__ == "__main__":
    main()
