"""REST API: ingest and query through the HTTP layer."""

from __future__ import annotations

from fastapi.testclient import TestClient

from logiq.api import AppState, create_app
from logiq.ingest import Ingestor
from logiq.store import Store
from logiq.wal import WriteAheadLog


def make_client(tmp_path) -> TestClient:
    store = Store(":memory:")
    wal = WriteAheadLog(str(tmp_path / "api.wal"))
    state = AppState(store, Ingestor(store, wal))
    return TestClient(create_app(state))


def test_ingest_then_query(tmp_path):
    client = make_client(tmp_path)
    body = {
        "records": [
            {
                "record_id": "a1",
                "source": "web",
                "timestamp": "2026-01-01T00:00:00+00:00",
                "level": "ERROR",
                "message": "request failed",
                "trace_id": "t1",
            }
        ]
    }
    resp = client.post("/ingest", json=body)
    assert resp.status_code == 200
    assert resp.json()["accepted"] == 1

    resp = client.get("/query", params={"level": "ERROR", "source": "web"})
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["records"]) == 1
    assert data["records"][0]["record_id"] == "a1"


def test_query_rejects_inverted_range(tmp_path):
    client = make_client(tmp_path)
    resp = client.get(
        "/query",
        params={"start": "2026-01-02T00:00:00+00:00", "end": "2026-01-01T00:00:00+00:00"},
    )
    assert resp.status_code == 400


def test_query_rejects_bad_limit(tmp_path):
    client = make_client(tmp_path)
    resp = client.get("/query", params={"limit": 0})
    assert resp.status_code == 422
