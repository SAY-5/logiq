"""REST API: HTTP push ingest and a paginated query endpoint.

Query parameters are validated by Pydantic and translated into a Query
object whose values are bound through SQL placeholders in the store. No
request value reaches SQL via string formatting.
"""

from __future__ import annotations

import os
from datetime import datetime

from fastapi import Depends, FastAPI, HTTPException
from fastapi import Query as QueryParam
from pydantic import BaseModel, Field

from .ingest import Ingestor
from .models import Level, LogRecord, Query
from .store import Store
from .wal import WriteAheadLog


class RecordIn(BaseModel):
    record_id: str = Field(min_length=1, max_length=512)
    source: str = Field(min_length=1, max_length=256)
    timestamp: datetime
    level: Level
    message: str = Field(max_length=65536)
    trace_id: str | None = Field(default=None, max_length=512)

    def to_record(self) -> LogRecord:
        return LogRecord(
            record_id=self.record_id,
            source=self.source,
            timestamp=self.timestamp,
            level=self.level,
            message=self.message,
            trace_id=self.trace_id,
        )


class IngestIn(BaseModel):
    records: list[RecordIn] = Field(min_length=1, max_length=10000)


class IngestOut(BaseModel):
    accepted: int
    batch_seq: int


class RecordOut(BaseModel):
    record_id: str
    source: str
    timestamp: datetime
    level: Level
    message: str
    trace_id: str | None


class QueryOut(BaseModel):
    records: list[RecordOut]
    next_offset: int | None
    scanned_partitions: int


class AppState:
    """Holds the store and ingestor for the process lifetime."""

    def __init__(self, store: Store, ingestor: Ingestor) -> None:
        self.store = store
        self.ingestor = ingestor


def build_state(db_path: str, wal_path: str) -> AppState:
    store = Store(db_path)
    wal = WriteAheadLog(wal_path)
    ingestor = Ingestor(store, wal)
    # Recover any in-flight batch from a previous run before serving.
    ingestor.recover()
    return AppState(store, ingestor)


def create_app(state: AppState | None = None) -> FastAPI:
    if state is None:
        db_path = os.environ.get("LOGIQ_DB", ":memory:")
        wal_path = os.environ.get("LOGIQ_WAL", "/tmp/logiq.wal")
        state = build_state(db_path, wal_path)

    app = FastAPI(title="LogIQ", version="0.4.0")

    def get_state() -> AppState:
        return state

    @app.get("/health")
    def health() -> dict[str, object]:
        return {"status": "ok", "records": get_state().store.count()}

    @app.post("/ingest", response_model=IngestOut)
    def ingest(body: IngestIn, st: AppState = Depends(get_state)) -> IngestOut:
        records = [r.to_record() for r in body.records]
        seq = st.ingestor.ingest(records)
        return IngestOut(accepted=len(records), batch_seq=seq)

    @app.get("/query", response_model=QueryOut)
    def query(
        st: AppState = Depends(get_state),
        start: datetime | None = None,
        end: datetime | None = None,
        source: str | None = None,
        level: Level | None = None,
        trace_id: str | None = None,
        text: str | None = QueryParam(default=None, max_length=4096),
        limit: int = QueryParam(default=100, ge=1, le=Query.MAX_LIMIT),
        offset: int = QueryParam(default=0, ge=0),
    ) -> QueryOut:
        try:
            q = Query(
                start=start,
                end=end,
                source=source,
                level=level,
                trace_id=trace_id,
                text=text,
                limit=limit,
                offset=offset,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        page = st.store.query(q)
        return QueryOut(
            records=[
                RecordOut(
                    record_id=r.record_id,
                    source=r.source,
                    timestamp=r.timestamp,
                    level=r.level,
                    message=r.message,
                    trace_id=r.trace_id,
                )
                for r in page.records
            ],
            next_offset=page.next_offset,
            scanned_partitions=page.total_scanned_partitions,
        )

    return app
