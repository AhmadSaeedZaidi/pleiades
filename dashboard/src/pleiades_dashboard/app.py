"""Same-origin UI and read-only API. No ingestion or paid API calls."""

from __future__ import annotations

import asyncio
import hmac
import html
import logging
import os
import re
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

STATIC = Path(__file__).parent / "static"
logger = logging.getLogger(__name__)
READ_ONLY_OPTIONS = (
    "-c default_transaction_read_only=on -c statement_timeout=5000 "
    "-c lock_timeout=1000 -c idle_in_transaction_session_timeout=5000 "
    "-c application_name=pleiades-dashboard"
)


async def service_status() -> dict[str, str]:
    """Probe the actual scheduler; recent DB activity alone is not service liveness."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "systemctl",
            "is-active",
            "pleiades-ingestion",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=2)
        except TimeoutError:
            proc.kill()
            await proc.wait()
            return {"state": "unknown", "label": "Status probe timed out"}
        state = stdout.decode().strip() or "unknown"
        return {"state": state, "label": "Ingestion scheduler"}
    except OSError:
        return {"state": "unknown", "label": "Service status unavailable"}


def create_app(repository: Any = None, *, demo: bool | None = None) -> FastAPI:
    if repository is None and demo is not True and os.getenv("PLEIADES_DASHBOARD_DEMO") != "1":
        load_dotenv(override=False)
    demo = os.getenv("PLEIADES_DASHBOARD_DEMO") == "1" if demo is None else demo
    token = os.getenv("PLEIADES_DASHBOARD_TOKEN", "")
    cache: dict[str, Any] = {}
    lock = asyncio.Lock()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool = None
        if repository is not None:
            app.state.repository = repository
        elif demo:
            from pleiades_dashboard.demo import DemoRepository

            app.state.repository = DemoRepository()
        else:
            dsn = os.getenv("PLEIADES_DASHBOARD_DATABASE_URL") or os.getenv("DATABASE_URL")
            if not dsn:
                raise RuntimeError("Set DATABASE_URL or launch with --demo")
            from atlas.repositories.dashboard import DashboardRepository

            pool = AsyncConnectionPool(
                dsn,
                min_size=0,
                max_size=2,
                timeout=5,
                open=False,
                kwargs={
                    "row_factory": dict_row,
                    "connect_timeout": 3,
                    "options": READ_ONLY_OPTIONS,
                },
            )
            await pool.open()
            app.state.repository = DashboardRepository(pool)
        try:
            yield
        finally:
            if pool is not None:
                await pool.close()

    app = FastAPI(title="Pleiades", lifespan=lifespan, docs_url=None, redoc_url=None)

    @app.middleware("http")
    async def boundary(request: Request, call_next: Any) -> Any:
        path = request.url.path.removeprefix(request.scope.get("root_path", ""))
        if path.startswith("/api/") and token:
            supplied = request.headers.get("Authorization", "").removeprefix("Bearer ")
            if not hmac.compare_digest(supplied.encode(), token.encode()):
                from fastapi.responses import JSONResponse

                return JSONResponse({"detail": "Dashboard access token required"}, status_code=401)
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' https://i.ytimg.com; connect-src 'self'; "
            "frame-ancestors 'none'; base-uri 'self'; form-action 'self'"
        )
        if path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
        return response

    async def read(method: str, **kwargs: Any) -> Any:
        try:
            return await getattr(app.state.repository, method)(**kwargs)
        except Exception as exc:
            # Do not return DB errors, DSNs, or exception text to the browser/logs.
            logger.warning("Dashboard query %s unavailable (%s)", method, type(exc).__name__)
            raise HTTPException(503, "Pipeline data unavailable. Check the dashboard service.")

    @app.get("/api/overview")
    async def overview() -> Any:
        async with lock:
            if cache.get("expires", 0) < time.monotonic():
                snapshot = await read("overview")
                cache.update(
                    data={
                        **snapshot,
                        "service": (
                            {"state": "demo", "label": "Sample environment"}
                            if demo
                            else await service_status()
                        ),
                        "demo": demo,
                        "updated_at": datetime.now(UTC),
                    },
                    expires=time.monotonic() + 15,
                )
            return cache["data"]

    @app.get("/api/videos")
    async def videos(
        q: Annotated[str, Query(max_length=120)] = "",
        status: Annotated[
            str, Query(pattern="^(PENDING|PROCESSING|PROCESSED|ARCHIVED|FAILED)?$")
        ] = "",
        stage: Annotated[str, Query(pattern="^(raw|audio|visuals|transcript|clip)?$")] = "",
        page: Annotated[int, Query(ge=1, le=1000)] = 1,
        page_size: Annotated[int, Query(ge=1, le=50)] = 25,
    ) -> Any:
        return await read(
            "videos", search=q, status=status, stage=stage, page=page, page_size=page_size
        )

    @app.get("/api/videos/{video_id}")
    async def video(video_id: str) -> Any:
        if not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
            raise HTTPException(422, "Use an 11-character YouTube video ID")
        data = await read("video", video_id=video_id)
        if data is None:
            raise HTTPException(404, "Video is not in the collection")
        return data

    @app.get("/api/events")
    async def events() -> Any:
        return {"items": await read("events")}

    @app.get("/api/queries")
    async def queries() -> Any:
        return {"items": await read("queries")}

    @app.get("/api/graph")
    async def graph(
        q: Annotated[str, Query(max_length=120)] = "",
        topic: Annotated[
            str,
            Query(
                max_length=1024,
                pattern=r"^(|https://[a-z][a-z0-9-]{1,15}\.wikipedia\.org/wiki/[^\s]+)$",
            ),
        ] = "",
        limit: Annotated[int, Query(ge=1, le=50)] = 25,
    ) -> Any:
        result = await read("graph", search=q, topic=topic, limit=limit)
        return {**result, "demo": demo}

    @app.get("/")
    async def index(request: Request) -> HTMLResponse:
        base = html.escape(request.scope.get("root_path", "").rstrip("/") + "/", quote=True)
        return HTMLResponse((STATIC / "index.html").read_text().replace("__PLEIADES_BASE__", base))

    @app.get("/healthz")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    app.mount("/static", StaticFiles(directory=STATIC), name="static")
    return app
