from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .errors import BusinessError
from .schemas import BatchRequest, MergeRequest, PhraseClause, RetireRequest, SearchRequest, TermClause
from .storage import IndexStore, SearchClause


logger = logging.getLogger(__name__)


DEFAULT_INDEX_FIELDS = (
    "title",
    "body",
    "tags",
    "author.name",
    "sections.title",
    "sections.paragraphs",
)


def load_index_fields() -> tuple[str, ...]:
    raw_fields = os.environ.get("INDEX_FIELDS")
    if raw_fields:
        value = json.loads(raw_fields)
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise ValueError("INDEX_FIELDS must be a JSON array of strings")
        return tuple(value)

    config_path = os.environ.get("INDEX_CONFIG_PATH")
    if config_path:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"INDEX_CONFIG_PATH does not exist: {path}")
    else:
        candidate = Path("samples/index_config.json")
        path = candidate if candidate.exists() else None
    if path is not None:
        data = json.loads(path.read_text(encoding="utf-8"))
        fields = data.get("index_fields")
        if not isinstance(fields, list) or not all(isinstance(item, str) for item in fields):
            raise ValueError("index config must contain a string array named index_fields")
        return tuple(fields)
    return DEFAULT_INDEX_FIELDS


def create_app(
    *,
    store: IndexStore | None = None,
    db_path: str | Path | None = None,
    index_fields: tuple[str, ...] | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if store is not None:
            app.state.store = store
        else:
            resolved_db = db_path or os.environ.get("INDEX_DB_PATH", "data/index.db")
            resolved_fields = index_fields if index_fields is not None else load_index_fields()
            app.state.store = IndexStore(resolved_db, resolved_fields)
        yield

    app = FastAPI(title="Versioned Local Inverted Index", version="1.0.0", lifespan=lifespan)

    def current_store(request: Request) -> IndexStore:
        return request.app.state.store

    @app.exception_handler(BusinessError)
    async def business_error_handler(_request: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.as_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "E_INVALID_REQUEST",
                    "message": "Request validation failed",
                    "details": {"errors": jsonable_encoder(exc.errors())},
                }
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = "E_NOT_FOUND" if exc.status_code == 404 else f"E_HTTP_{exc.status_code}"
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": {"code": code, "message": str(exc.detail)}},
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled API error", exc_info=exc)
        return JSONResponse(
            status_code=500,
            content={"error": {"code": "E_INTERNAL", "message": "Internal server error"}},
        )

    @app.get("/health")
    def health(request: Request) -> dict[str, Any]:
        index = current_store(request)
        return {
            "status": "ok",
            "current_version": index.current_version(),
            "query_activity": index.query_activity(),
        }

    @app.post("/v1/batches", status_code=201)
    def write_batch(body: BatchRequest, request: Request) -> dict[str, Any]:
        result = current_store(request).write_batch(
            inserts=body.inserts,
            updates=body.updates,
            deletes=body.deletes,
            batch_id=body.batch_id,
        )
        return result.as_dict()

    @app.post("/v1/search")
    def search(body: SearchRequest, request: Request) -> dict[str, Any]:
        clauses: list[SearchClause] = []
        for clause in body.clauses:
            if isinstance(clause, TermClause):
                clauses.append(
                    SearchClause(kind="term", field_path=clause.field, term=clause.term)
                )
            elif isinstance(clause, PhraseClause):
                clauses.append(
                    SearchClause(
                        kind="phrase",
                        field_path=clause.field,
                        terms=tuple(clause.terms),
                    )
                )
        result = current_store(request).search(
            clauses=clauses,
            operator=body.operator,
            limit=body.limit,
            offset=body.offset,
            version=body.version,
        )
        return result.as_dict()

    @app.get("/v1/postings")
    def postings(
        request: Request,
        path: str = Query(..., alias="path"),
        term: str = Query(...),
        version: int | None = Query(default=None, ge=0),
    ) -> dict[str, Any]:
        return current_store(request).get_postings(path, term, version).as_dict()

    @app.get("/v1/documents/{doc_id}")
    def document(doc_id: str, request: Request, version: int | None = Query(default=None, ge=0)) -> dict[str, Any]:
        value = current_store(request).get_document(doc_id, version)
        if value is None:
            raise BusinessError(
                "E_DOC_NOT_FOUND",
                "Document does not exist at this version",
                status_code=404,
                details={"doc_id": doc_id},
            )
        return {"_id": doc_id, "document": value}

    @app.get("/v1/versions/current")
    def current_version(request: Request) -> dict[str, Any]:
        index = current_store(request)
        return {"current_version": index.current_version()}

    @app.get("/v1/versions")
    def versions(request: Request) -> dict[str, Any]:
        return {"versions": current_store(request).list_versions()}

    @app.get("/v1/index/segments")
    def segments(request: Request, include_merged: bool = Query(default=True)) -> dict[str, Any]:
        return {
            "segments": current_store(request).list_segments(include_merged=include_merged),
            "query_activity": current_store(request).query_activity(),
        }

    @app.get("/v1/index/fields")
    def get_index_fields(request: Request) -> dict[str, Any]:
        return {"index_fields": current_store(request).list_index_fields()}

    @app.post("/v1/index/merge")
    def merge(body: MergeRequest, request: Request) -> dict[str, Any]:
        return current_store(request).merge_segments(body.source_segment_ids).as_dict()

    @app.post("/v1/versions/retire")
    def retire(body: RetireRequest, request: Request) -> dict[str, Any]:
        return current_store(request).retire_versions(retain_versions=body.retain_versions)

    @app.get("/v1/recovery/report")
    def recovery_report(request: Request) -> dict[str, Any]:
        report = current_store(request).recovery_report()
        if report is None:
            raise BusinessError("E_REPORT_NOT_FOUND", "No recovery report exists", status_code=404)
        return report

    @app.post("/v1/recovery/run")
    def run_recovery(request: Request) -> dict[str, Any]:
        return current_store(request).recover()

    return app


app = create_app()
