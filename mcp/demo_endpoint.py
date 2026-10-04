#!/usr/bin/env python3
"""Self-contained demo: boots the Pleiades MCP HTTP server, hits its endpoint
over real HTTP, prints the response, then shuts down.

Run with:
    .venv/bin/python mcp/demo_endpoint.py
"""

import os
import sys
import time
import urllib.request
import urllib.error

# Env (not queried, but atlas.config validates DATABASE_URL at import).
os.environ.setdefault("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
os.environ.setdefault("YOUTUBE_API_KEY_POOL_JSON", '["test_key_1"]')
os.environ.setdefault("VAULT_PROVIDER", "huggingface")
os.environ.setdefault("HF_DATASET_ID", "mock/ds")
os.environ.setdefault("HF_TOKEN", "mock")
os.environ.setdefault("PLEIADES_MCP_ARTIFACT_DIR", "/tmp/pmcp_demo_artifacts")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
sys.path.insert(0, os.path.abspath("atlas/src"))
sys.path.insert(0, os.path.abspath("maia/src"))

import threading
import uvicorn

import pleiades_mcp.server as srv


def _wait_http(url: str, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=1)
            return True
        except urllib.error.HTTPError:
            # Any HTTP response means the server is listening.
            return True
        except (urllib.error.URLError, ConnectionError, OSError):
            time.sleep(0.2)
    return False


def main() -> None:
    srv._http_base = "http://127.0.0.1:8000"
    uvicorn_server = uvicorn.Server(
        uvicorn.Config(
            srv.mcp.streamable_http_app(),
            host="127.0.0.1",
            port=8000,
            log_level="warning",
        )
    )
    t = threading.Thread(target=uvicorn_server.run, daemon=True)
    t.start()

    if not _wait_http("http://127.0.0.1:8000/", timeout=15):
        print("ERROR: server did not start")
        uvicorn_server.should_exit = True
        return

    print("MCP server is up at http://127.0.0.1:8000/")
    print("Transport: Streamable HTTP (MCP). Tools are invoked via the MCP protocol,")
    print("not plain REST, so a client (Claude Desktop, an MCP client, or the SDK)")
    print("connects to this endpoint and calls tools like `search_youtube`.")
    print()
    print("To connect from an MCP client, use this server URL:")
    print("    http://127.0.0.1:8000/mcp")
    print()
    # Confirm the ASGI app responds (200 even for an unknown path is fine).
    try:
        with urllib.request.urlopen("http://127.0.0.1:8000/mcp", timeout=3) as r:
            print(f"GET /mcp -> HTTP {r.status}")
    except urllib.error.HTTPError as e:
        print(f"GET /mcp -> HTTP {e.code} (expected; needs MCP handshake)")

    uvicorn_server.should_exit = True
    t.join(timeout=5)
    print("Server stopped.")


if __name__ == "__main__":
    main()
