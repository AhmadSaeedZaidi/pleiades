#!/usr/bin/env python3
"""Detached launcher for the Pleiades MCP HTTP server.

Double-forks so the server survives the spawning shell's session, then execs
the uvicorn-based FastMCP streamable-http server plus the artifact static
server. Logs to /tmp/pmcp_server.log.
"""

import os
import sys
import time

LOG = "/tmp/pmcp_server.log"


def main() -> None:
    # First fork.
    if os.fork() > 0:
        return
    os.setsid()
    # Second fork.
    if os.fork() > 0:
        sys.exit(0)

    os.chdir("/home/ubuntu/code/pleiades")
    os.umask(0)

    # Redirect std streams to the log.
    fd = os.open(LOG, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    os.dup2(fd, 1)
    os.dup2(fd, 2)
    devnull = os.open(os.devnull, os.O_RDONLY)
    os.dup2(devnull, 0)

    env = dict(os.environ)
    env.setdefault("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    env.setdefault("YOUTUBE_API_KEY_POOL_JSON", '["test_key_1"]')
    env.setdefault("VAULT_PROVIDER", "huggingface")
    env.setdefault("HF_DATASET_ID", "mock/ds")
    env.setdefault("HF_TOKEN", "mock")
    env.setdefault("PLEIADES_MCP_ARTIFACT_DIR", "/tmp/pmcp_artifacts")
    os.makedirs(env["PLEIADES_MCP_ARTIFACT_DIR"], exist_ok=True)

    # Launch the server under the venv interpreter.
    os.execvpe(
        "/home/ubuntu/code/pleiades/.venv/bin/python",
        [
            "/home/ubuntu/code/pleiades/.venv/bin/python",
            "-m",
            "pleiades_mcp.server",
            "--transport",
            "streamable-http",
            "--host",
            "127.0.0.1",
            "--port",
            "8000",
            "--log-level",
            "INFO",
        ],
        env,
    )


if __name__ == "__main__":
    main()
    # Give the child a moment, then the parent exits and the tool returns.
    time.sleep(0.2)
    print("launched")
