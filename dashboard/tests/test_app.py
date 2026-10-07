from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from pleiades_dashboard.app import READ_ONLY_OPTIONS, create_app
from pleiades_dashboard.demo import DemoRepository


def test_demo_requires_no_database_and_all_views_work(monkeypatch):
    monkeypatch.delenv("PLEIADES_DASHBOARD_TOKEN", raising=False)
    with patch("pleiades_dashboard.app.load_dotenv") as load:
        app = create_app(demo=True)
        load.assert_not_called()
    with TestClient(app) as client:
        assert client.get("/").status_code == 200
        assert client.get("/static/app.js").status_code == 200
        assert client.get("/api/overview").json()["demo"] is True
        videos = client.get("/api/videos").json()
        assert len(videos["items"]) == 8
        assert client.get("/api/videos/demo0000000").json()["transcript_preview"]
        assert client.get("/api/events").json()["items"]
        assert client.get("/api/queries").json()["items"]


@pytest.mark.parametrize(
    "query",
    ["page=0", "page=1001", "page_size=51", "stage=raw;DROP", "status=MADEUP", "q=" + "a" * 121],
)
def test_invalid_filters_never_reach_repository(monkeypatch, query):
    monkeypatch.delenv("PLEIADES_DASHBOARD_TOKEN", raising=False)
    repo = MagicMock()
    with TestClient(create_app(repo)) as client:
        assert client.get("/api/videos?" + query).status_code == 422
    repo.videos.assert_not_called()


def test_search_stage_filter_and_pagination(monkeypatch):
    monkeypatch.delenv("PLEIADES_DASHBOARD_TOKEN", raising=False)
    with TestClient(create_app(DemoRepository(), demo=True)) as client:
        assert client.get("/api/videos?q=coffee").json()["total"] == 1
        assert client.get("/api/videos?stage=visuals").json()["total"] == 1
        assert (
            client.get("/api/videos?page=2&page_size=3").json()["items"][0]["id"] == "demo0000003"
        )
        assert client.get("/api/videos/abcdefghijk").status_code == 404
        assert client.get("/api/videos/invalid").status_code == 422


def test_token_protects_every_api_without_exposing_it(monkeypatch):
    monkeypatch.setenv("PLEIADES_DASHBOARD_TOKEN", "private-token")
    with TestClient(create_app(demo=True)) as client:
        assert client.get("/").status_code == 200
        for route in ["overview", "videos", "videos/demo0000000", "events", "queries", "graph"]:
            response = client.get("/api/" + route)
            assert response.status_code == 401
            assert "private-token" not in response.text
            assert (
                client.get(
                    "/api/" + route, headers={"Authorization": "Bearer private-token"}
                ).status_code
                == 200
            )


def test_database_errors_are_redacted_and_writes_are_absent(monkeypatch):
    monkeypatch.delenv("PLEIADES_DASHBOARD_TOKEN", raising=False)
    repo = MagicMock(events=AsyncMock(side_effect=RuntimeError("postgres://SECRET@db")))
    with TestClient(create_app(repo)) as client:
        response = client.get("/api/events")
        assert response.status_code == 503
        assert "SECRET" not in response.text
        for route in ["overview", "videos", "events", "queries", "graph"]:
            assert client.post("/api/" + route, json={}).status_code == 405


def test_overview_cache_does_not_repeat_expensive_queries(monkeypatch):
    monkeypatch.delenv("PLEIADES_DASHBOARD_TOKEN", raising=False)
    repo = MagicMock(overview=AsyncMock(return_value={"counts": {}}))
    with patch(
        "pleiades_dashboard.app.service_status", AsyncMock(return_value={"state": "active"})
    ):
        with TestClient(create_app(repo)) as client:
            first = client.get("/api/overview").json()
            assert client.get("/api/overview").json() == first
    repo.overview.assert_awaited_once()


def test_live_pool_is_small_read_only_and_closed(monkeypatch):
    monkeypatch.delenv("PLEIADES_DASHBOARD_TOKEN", raising=False)
    pool = MagicMock(open=AsyncMock(), close=AsyncMock())
    with patch("pleiades_dashboard.app.AsyncConnectionPool", return_value=pool) as factory:
        with TestClient(create_app(demo=False)):
            kwargs = factory.call_args.kwargs
            assert kwargs["max_size"] == 2
            assert kwargs["kwargs"]["options"] == READ_ONLY_OPTIONS
            assert "default_transaction_read_only=on" in READ_ONLY_OPTIONS
            assert "statement_timeout=5000" in READ_ONLY_OPTIONS
        pool.close.assert_awaited_once()


def test_api_security_headers(monkeypatch):
    monkeypatch.delenv("PLEIADES_DASHBOARD_TOKEN", raising=False)
    with TestClient(create_app(demo=True)) as client:
        response = client.get("/api/events")
        assert response.headers["Cache-Control"] == "no-store"
        assert response.headers["X-Content-Type-Options"] == "nosniff"
        assert "frame-ancestors 'none'" in response.headers["Content-Security-Policy"]


def test_proxy_prefix_preserves_assets_and_token_boundary(monkeypatch):
    monkeypatch.setenv("PLEIADES_DASHBOARD_TOKEN", "private-token")
    with TestClient(create_app(demo=True), root_path="/pleiades") as client:
        page = client.get("/pleiades/")
        assert '<base href="/pleiades/">' in page.text
        assert client.get("/pleiades/static/app.js").status_code == 200
        assert client.get("/pleiades/healthz").json() == {"status": "ok"}
        response = client.get("/pleiades/api/overview")
        assert response.status_code == 401
        assert (
            client.get(
                "/pleiades/api/overview", headers={"Authorization": "Bearer private-token"}
            ).json()["demo"]
            is True
        )


@pytest.mark.parametrize(
    "query", ["limit=0", "limit=51", "q=" + "a" * 121, "topic=https://evil.example/wiki/Science"]
)
def test_graph_invalid_bounds_never_reach_repository(monkeypatch, query):
    monkeypatch.delenv("PLEIADES_DASHBOARD_TOKEN", raising=False)
    repo = MagicMock()
    with TestClient(create_app(repo)) as client:
        assert client.get("/api/graph?" + query).status_code == 422
    repo.graph.assert_not_called()


def test_demo_graph_exports_are_marked_and_topic_search_works(monkeypatch):
    monkeypatch.delenv("PLEIADES_DASHBOARD_TOKEN", raising=False)
    with TestClient(create_app(demo=True), root_path="/pleiades") as client:
        result = client.get("/pleiades/api/graph?q=science&limit=10").json()
        assert result["demo"] and result["sampled"]
        assert result["schema"] == "pleiades.topic-graph.v1"
        assert len(result["topics"]) == 1
        assert result["nodes"] and result["edges"]
        assert client.get("/pleiades/static/graph.js").status_code == 200
        assert client.post("/pleiades/api/graph").status_code == 405
