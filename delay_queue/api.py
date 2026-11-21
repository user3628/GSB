from __future__ import annotations

import re
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Body, FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .config import Settings
from .errors import E_INTERNAL, E_NOT_FOUND, E_TASK_INVALID, BusinessError
from .service import DelayQueueService


QUEUE_RE = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")


def _validate_queue_name(queue_name: str) -> None:
    if not isinstance(queue_name, str) or not QUEUE_RE.fullmatch(queue_name):
        raise BusinessError(
            E_TASK_INVALID,
            "queue name must match ^[A-Za-z0-9_.-]{1,100}$",
            {"queue": queue_name},
            422,
        )


def create_app(settings: Settings | None = None) -> FastAPI:
    config = settings or Settings.from_env()
    config.validate()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service = DelayQueueService(config)
        service.startup()
        app.state.service = service
        yield

    app = FastAPI(
        title="Durable Local Delayed Queue",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.exception_handler(BusinessError)
    async def business_error_handler(request: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.as_dict())

    @app.exception_handler(RequestValidationError)
    async def request_validation_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "code": E_TASK_INVALID,
                "message": "request body validation failed",
                "context": {"errors": jsonable_encoder(exc.errors())},
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        code = E_NOT_FOUND if exc.status_code == 404 else E_INTERNAL
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "ok": False,
                "code": code,
                "message": str(exc.detail),
                "context": {"path": request.url.path},
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "code": E_INTERNAL,
                "message": "internal server error",
                "context": {"type": type(exc).__name__, "path": request.url.path},
            },
        )

    def service(request: Request) -> DelayQueueService:
        return request.app.state.service

    @app.get("/healthz")
    def health() -> dict[str, Any]:
        return {"ok": True, "service": "durable-local-delayed-queue"}

    @app.post("/api/v1/queues/{queue_name}/tasks", status_code=201)
    async def submit_tasks(
        queue_name: str, request: Request, body: Any = Body(...)
    ) -> dict[str, Any]:
        _validate_queue_name(queue_name)
        if isinstance(body, dict):
            tasks = body.get("tasks")
            default_endpoint = body.get("endpoint")
        elif isinstance(body, list):
            tasks = body
            default_endpoint = None
        else:
            tasks = None
            default_endpoint = None
        return await service(request).submit_tasks_locked(
            queue_name, tasks, default_endpoint
        )

    @app.post("/api/v1/queues/{queue_name}/rounds")
    @app.post("/api/v1/queues/{queue_name}/schedule")
    async def trigger_round(
        queue_name: str, request: Request, body: Any = Body(...)
    ) -> dict[str, Any]:
        _validate_queue_name(queue_name)
        if not isinstance(body, dict):
            raise BusinessError(
                "E_INVALID_CONFIG",
                "round request body must be an object",
                {"field": "body"},
                422,
            )
        round_id = body.get("round_id")
        config_body = body.get("config", body)
        if not isinstance(config_body, dict):
            raise BusinessError(
                "E_INVALID_CONFIG",
                "config must be an object",
                {"field": "config"},
                422,
            )
        if "round_id" in config_body:
            config_body = {k: v for k, v in config_body.items() if k != "round_id"}
        return await service(request).trigger_round(queue_name, round_id, config_body)

    @app.post("/api/v1/queues/{queue_name}/rounds/{round_id}")
    async def trigger_named_round(
        queue_name: str, round_id: str, request: Request, body: Any = Body(default=None)
    ) -> dict[str, Any]:
        _validate_queue_name(queue_name)
        if isinstance(body, dict) and "config" in body:
            body = body["config"]
        return await service(request).trigger_round(queue_name, round_id, body)

    @app.get("/api/v1/queues/{queue_name}/rounds/{round_id}")
    def get_round(queue_name: str, round_id: str, request: Request) -> dict[str, Any]:
        _validate_queue_name(queue_name)
        row = service(request).get_round_for_queue(queue_name, round_id)
        return {"ok": True, "round": row, "reused": True}

    @app.get("/api/v1/queues/{queue_name}/status")
    @app.get("/api/v1/queues/{queue_name}")
    @app.get("/api/v1/queues/{queue_name}/state")
    def queue_status(queue_name: str, request: Request) -> dict[str, Any]:
        _validate_queue_name(queue_name)
        return service(request).get_queue_status(queue_name)

    @app.get("/api/v1/queues/{queue_name}/tasks/{task_id}")
    def task_trajectory(
        queue_name: str, task_id: str, request: Request
    ) -> dict[str, Any]:
        _validate_queue_name(queue_name)
        return service(request).get_task_trajectory(queue_name, task_id)

    @app.get("/api/v1/queues/{queue_name}/tasks/{task_id}/trajectory")
    @app.get("/api/v1/queues/{queue_name}/tasks/{task_id}/history")
    def task_trajectory_alias(
        queue_name: str, task_id: str, request: Request
    ) -> dict[str, Any]:
        _validate_queue_name(queue_name)
        return service(request).get_task_trajectory(queue_name, task_id)

    @app.post("/api/v1/queues/{queue_name}/tasks/{task_id}/transitions")
    async def manual_transition(
        queue_name: str,
        task_id: str,
        request: Request,
        body: dict[str, Any] = Body(...),
    ) -> dict[str, Any]:
        _validate_queue_name(queue_name)
        to_state = body.get("to_state", body.get("target_state"))
        reason = body.get("reason")
        if not isinstance(to_state, str) or not isinstance(reason, str) or not reason.strip():
            raise BusinessError(
                E_TASK_INVALID,
                "to_state and a non-empty reason are required",
                {"fields": ["to_state", "reason"]},
                422,
            )
        task = await service(request).manual_transition_locked(
            queue_name, task_id, to_state, reason
        )
        return {"ok": True, "task": task}

    @app.get("/api/v1/recovery/report")
    @app.get("/api/v1/recovery")
    def recovery_report(request: Request, queue: str | None = None) -> dict[str, Any]:
        if queue is not None:
            _validate_queue_name(queue)
        return service(request).get_recovery_report(queue)

    @app.get("/api/v1/queues/{queue_name}/recovery-report")
    def queue_recovery_report(queue_name: str, request: Request) -> dict[str, Any]:
        _validate_queue_name(queue_name)
        return service(request).get_recovery_report(queue_name)

    @app.get("/api/v1/rounds/{round_id}")
    def global_round(round_id: str, request: Request) -> dict[str, Any]:
        return {"ok": True, "round": service(request).get_round(round_id)}

    @app.post("/api/v1/tasks", status_code=201)
    async def submit_tasks_global(
        request: Request, body: Any = Body(...)
    ) -> dict[str, Any]:
        if not isinstance(body, dict):
            raise BusinessError(
                E_TASK_INVALID,
                "request body must include queue and tasks",
                {"field": "body"},
                422,
            )
        queue_name = body.get("queue", body.get("queue_name"))
        _validate_queue_name(queue_name)
        return await service(request).submit_tasks_locked(
            queue_name, body.get("tasks"), body.get("endpoint")
        )

    @app.post("/api/v1/schedule")
    async def trigger_round_global(
        request: Request, body: Any = Body(...)
    ) -> dict[str, Any]:
        if not isinstance(body, dict):
            raise BusinessError(
                "E_INVALID_CONFIG",
                "request body must include queue and round_id",
                {"field": "body"},
                422,
            )
        queue_name = body.get("queue", body.get("queue_name"))
        _validate_queue_name(queue_name)
        round_id = body.get("round_id")
        config_body = body.get("config", body)
        if not isinstance(config_body, dict):
            raise BusinessError(
                "E_INVALID_CONFIG", "config must be an object", {}, 422
            )
        config_body = {
            key: value for key, value in config_body.items() if key != "round_id"
        }
        return await service(request).trigger_round(queue_name, round_id, config_body)

    return app


app = create_app()

