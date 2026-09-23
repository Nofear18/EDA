"""FastAPI schemas and routes for the versioned interactive control plane."""

from __future__ import annotations

import logging
import math
from collections.abc import Callable
from typing import Any, Literal

from fastapi import APIRouter, FastAPI, Query, Request
from fastapi.exception_handlers import (
    http_exception_handler,
    request_validation_exception_handler,
)
from fastapi.exceptions import RequestValidationError, ResponseValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator
from starlette.exceptions import HTTPException as StarletteHTTPException

from science_eda.config import SandboxConfig
from science_eda.exceptions import (
    InteractiveInternalError,
    InteractiveInvalidRequestError,
    InteractiveSandboxError,
)
from science_eda.sandbox.interactive.models import SCHEMA_VERSION, new_id
from science_eda.sandbox.interactive.registry_constants import SQLITE_INTEGER_MAX
from science_eda.sandbox.interactive.tool_config import validate_tool_version
from science_eda.sandbox.interactive.workspace import validate_requested_session_id

logger = logging.getLogger(__name__)


def _bounded_utf8(value: object, max_bytes: int = 4096) -> str:
    encoded = str(value).encode("utf-8", errors="replace")
    if len(encoded) <= max_bytes:
        return encoded.decode("utf-8")
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


class _StrictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class _StrictResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class CreateWorkspaceSessionBody(_StrictRequest):
    session_id: str | None = None
    tool_kind: Literal["innovus", "primetime"]
    version: str | None = None
    workspace_path: str

    @field_validator("session_id")
    @classmethod
    def validate_session_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            return validate_requested_session_id(value)
        except InteractiveInvalidRequestError as exc:
            raise ValueError(str(exc)) from exc

    @field_validator("version")
    @classmethod
    def validate_version(cls, value: str | None) -> str | None:
        try:
            return validate_tool_version(value)
        except InteractiveInvalidRequestError as exc:
            raise ValueError(str(exc)) from exc

    @field_validator("workspace_path")
    @classmethod
    def validate_workspace_path_text(cls, value: str) -> str:
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError("workspace_path must be valid UTF-8 text") from exc
        return value


class ExecuteWorkspaceSessionBody(_StrictRequest):
    request_id: str
    code: str
    timeout_ms: int | None = Field(
        default=None,
        gt=0,
        le=SQLITE_INTEGER_MAX,
    )

    @field_validator("request_id")
    @classmethod
    def validate_request_id(cls, value: str) -> str:
        return _validate_request_id(value)


class WorkspacePathPolicyResponse(_StrictResponse):
    required: bool
    kind: Literal["local_absolute_directory"]
    canonicalization: Literal["realpath"]
    runtime_initial_cwd: bool
    managed_root_overlap: Literal["reject"]
    filesystem_scope: Literal["host_user"]


class CapabilityLimitsResponse(_StrictResponse):
    max_code_bytes: int
    output_preview_max_bytes: int
    execution_log_max_bytes: int
    session_log_max_bytes: int
    create_request_retention_seconds: int
    history_default_page_size: int
    history_max_page_size: int
    history_page_max_bytes: int


class CapabilitiesResponse(_StrictResponse):
    schema_version: Literal["1"]
    deployment_mode: Literal["trusted_local"]
    shared_filesystem: bool
    local_log_root: str
    workspace_path_policy: WorkspacePathPolicyResponse
    supported_tool_kinds: list[Literal["innovus", "primetime"]]
    limits: CapabilityLimitsResponse


class SessionRuntimeResponse(_StrictResponse):
    state: Literal["READY", "BUSY", "LOST", "STOPPED"]
    runtime_instance_id: str
    process_id: int | None
    current_execution_id: str | None
    lost_reason: str | None
    lost_at: str | None


class SessionTimestampsResponse(_StrictResponse):
    created_at: str
    last_active_at: str
    idle_expires_at: str | None


class WorkspaceSessionResponse(_StrictResponse):
    workspace_session_id: str
    tool_kind: Literal["innovus", "primetime"]
    version: str | None
    workspace_path: str
    state: Literal["ACTIVE", "CLOSING", "CLOSED"]
    runtime: SessionRuntimeResponse
    timestamps: SessionTimestampsResponse


class CreateWorkspaceSessionResponse(_StrictResponse):
    schema_version: Literal["1"]
    session_id: str
    workspace_session: WorkspaceSessionResponse


class WorkspaceSessionSummaryResponse(_StrictResponse):
    workspace_session_id: str
    tool_kind: Literal["innovus", "primetime"]
    version: str | None
    workspace_path: str
    state: Literal["ACTIVE", "CLOSING", "CLOSED"]
    runtime_state: Literal["READY", "BUSY", "LOST", "STOPPED"]
    current_execution_id: str | None
    created_at: str
    last_active_at: str
    idle_expires_at: str | None


class ListWorkspaceSessionsResponse(_StrictResponse):
    schema_version: Literal["1"]
    count: int
    sessions: list[WorkspaceSessionSummaryResponse]


class GetWorkspaceSessionResponse(_StrictResponse):
    schema_version: Literal["1"]
    workspace_session: WorkspaceSessionResponse


class ExecutionTimingResponse(_StrictResponse):
    submitted_at: str
    started_at: str | None
    ended_at: str | None
    duration_ms: int | None


class ExecutionErrorResponse(_StrictResponse):
    code: str
    message: str


class FullLogResponse(_StrictResponse):
    ref: str
    access: Literal["local_file"]
    path: str
    complete: bool | None
    written_bytes: int | None
    dropped_bytes: int | None
    incomplete_reason: str | None


class HistoryOutputResponse(_StrictResponse):
    preview: str | None
    total_bytes: int | None
    total_lines: int | None
    truncated: bool | None
    full_log: FullLogResponse


class HistoryResultSummaryResponse(_StrictResponse):
    exit_code: int | None
    error: ExecutionErrorResponse | None
    output: HistoryOutputResponse


class HistoryItemResponse(_StrictResponse):
    sequence: int
    execution_id: str
    request_id: str
    state: Literal["RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "LOST"]
    code: str
    timing: ExecutionTimingResponse
    result_summary: HistoryResultSummaryResponse


class HistoryPageResponse(_StrictResponse):
    count: int
    has_more: bool
    next_cursor: str | None


class WorkspaceSessionHistoryResponse(_StrictResponse):
    schema_version: Literal["1"]
    workspace_session_id: str
    order: Literal["sequence_asc"]
    history: list[HistoryItemResponse]
    page: HistoryPageResponse


class ExecutionOutputResponse(HistoryOutputResponse):
    preview_strategy: str | None
    returned_bytes: int | None


class ExecutionResultResponse(_StrictResponse):
    exit_code: int | None
    error: ExecutionErrorResponse | None
    output: ExecutionOutputResponse


class ExecutionRuntimeResponse(_StrictResponse):
    state: Literal["READY", "BUSY", "LOST", "STOPPED"]
    runtime_instance_id: str
    preserved: bool | None
    lost_reason: str | None
    lost_at: str | None


class ExecuteWorkspaceSessionResponse(_StrictResponse):
    schema_version: Literal["1"]
    execution_id: str
    request_id: str
    workspace_session_id: str
    tool_kind: Literal["innovus", "primetime"]
    state: Literal["RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "LOST"]
    timing: ExecutionTimingResponse
    result: ExecutionResultResponse
    runtime: ExecutionRuntimeResponse


class DestroyWorkspaceSessionResponse(_StrictResponse):
    schema_version: Literal["1"]
    workspace_session_id: str
    state: Literal["CLOSED"]
    runtime_terminated: bool
    closed_at: str | None


def _validate_request_id(value: str) -> str:
    if not value or not value.strip():
        raise ValueError("request_id must not be empty")
    if "\x00" in value:
        raise ValueError("request_id must not contain NUL bytes")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("request_id must be valid UTF-8 text") from exc
    if len(encoded) > 256:
        raise ValueError("request_id exceeds 256 UTF-8 bytes")
    return value


def _with_schema(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("schema_version") == SCHEMA_VERSION:
        return payload
    return {**payload, "schema_version": SCHEMA_VERSION}


def _is_v1_path(path: str) -> bool:
    return path == "/v1" or path.startswith("/v1/")


def _invoke(
    function: Callable[[], dict[str, Any]],
    *,
    request_id: str | None = None,
    session_id: str | None = None,
    workspace_session_id: str | None = None,
) -> dict[str, Any]:
    try:
        return _with_schema(function())
    except InteractiveSandboxError as exc:
        if session_id is not None:
            # Create no longer exposes its internal durable request key.
            exc.request_id = None
            exc.session_id = session_id
        elif getattr(exc, "request_id", None) is None:
            exc.request_id = request_id
        if getattr(exc, "workspace_session_id", None) is None:
            exc.workspace_session_id = workspace_session_id
        raise
    except Exception as exc:  # noqa: BLE001 - sanitize unexpected daemon errors
        logger.exception("interactive sandbox request failed")
        raise InteractiveInternalError(
            "internal interactive sandbox error",
            request_id=request_id,
            session_id=session_id,
            workspace_session_id=workspace_session_id,
        ) from exc


def build_interactive_router(service: Any, config: SandboxConfig) -> APIRouter:
    router = APIRouter(prefix="/v1")

    @router.get("/capabilities", response_model=CapabilitiesResponse)
    def capabilities() -> dict[str, Any]:
        return _invoke(service.capabilities)

    @router.post(
        "/workspace-sessions/create",
        response_model=CreateWorkspaceSessionResponse,
    )
    def create_workspace_session(body: CreateWorkspaceSessionBody) -> dict[str, Any]:
        session_id = body.session_id if body.session_id is not None else new_id("wss")
        return _invoke(
            lambda: service.create_workspace_session(
                session_id=session_id,
                tool_kind=body.tool_kind,
                version=body.version,
                workspace_path=body.workspace_path,
            ),
            session_id=session_id,
        )

    @router.get(
        "/workspace-sessions/list",
        response_model=ListWorkspaceSessionsResponse,
    )
    def list_workspace_sessions() -> dict[str, Any]:
        return _invoke(service.list_workspace_sessions)

    @router.get(
        "/workspace-sessions/{workspace_session_id}",
        response_model=GetWorkspaceSessionResponse,
    )
    def get_workspace_session(workspace_session_id: str) -> dict[str, Any]:
        return _invoke(
            lambda: service.get_workspace_session(workspace_session_id),
            workspace_session_id=workspace_session_id,
        )

    @router.get(
        "/workspace-sessions/{workspace_session_id}/history",
        response_model=WorkspaceSessionHistoryResponse,
    )
    def workspace_session_history(
        workspace_session_id: str,
        limit: int | None = Query(default=None, ge=1),
        cursor: str | None = Query(default=None),
    ) -> dict[str, Any]:
        effective_limit = (
            int(limit)
            if limit is not None
            else int(config.interactive_history_default_page_size)
        )
        return _invoke(
            lambda: service.workspace_session_history(
                workspace_session_id,
                limit=effective_limit,
                cursor=cursor,
            ),
            workspace_session_id=workspace_session_id,
        )

    @router.delete(
        "/workspace-sessions/{workspace_session_id}",
        response_model=DestroyWorkspaceSessionResponse,
    )
    def destroy_workspace_session(workspace_session_id: str) -> dict[str, Any]:
        return _invoke(
            lambda: service.destroy_workspace_session(workspace_session_id),
            workspace_session_id=workspace_session_id,
        )

    @router.post(
        "/workspace-sessions/{workspace_session_id}:execute",
        response_model=ExecuteWorkspaceSessionResponse,
    )
    def execute_workspace_session(
        workspace_session_id: str,
        body: ExecuteWorkspaceSessionBody,
    ) -> dict[str, Any]:
        if body.timeout_ms is not None:
            timeout_ms = int(body.timeout_ms)
        else:
            try:
                timeout_seconds = float(config.interactive_execute_timeout)
                if not math.isfinite(timeout_seconds):
                    raise ValueError
                timeout_ms = round(timeout_seconds * 1000)
            except (OverflowError, TypeError, ValueError) as exc:
                raise InteractiveInvalidRequestError(
                    "configured default timeout cannot be represented as timeout_ms",
                    request_id=body.request_id,
                    workspace_session_id=workspace_session_id,
                ) from exc
            if timeout_ms <= 0 or timeout_ms > SQLITE_INTEGER_MAX:
                raise InteractiveInvalidRequestError(
                    f"timeout_ms must be an integer from 1 to {SQLITE_INTEGER_MAX}",
                    request_id=body.request_id,
                    workspace_session_id=workspace_session_id,
                )
        return _invoke(
            lambda: service.execute_workspace_session(
                workspace_session_id,
                request_id=body.request_id,
                code=body.code,
                timeout_ms=timeout_ms,
            ),
            request_id=body.request_id,
            workspace_session_id=workspace_session_id,
        )

    return router


def install_interactive_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ResponseValidationError)
    async def interactive_response_validation_handler(
        request: Request,
        exc: ResponseValidationError,
    ) -> JSONResponse:
        if not _is_v1_path(request.url.path):
            # Preserve FastAPI's normal legacy behavior: response validation is
            # a server exception, not a client-facing validation envelope.
            raise exc
        logger.error(
            "interactive sandbox response failed schema validation for %s",
            request.url.path,
        )
        error: dict[str, Any] = {
            "code": "INTERNAL_ERROR",
            "message": "internal interactive sandbox response error",
            "details": {},
        }
        workspace_session_id = request.path_params.get("workspace_session_id")
        if workspace_session_id:
            error["workspace_session_id"] = str(workspace_session_id)
        return JSONResponse(
            status_code=500,
            content={"schema_version": SCHEMA_VERSION, "error": error},
        )

    @app.exception_handler(StarletteHTTPException)
    async def interactive_http_error_handler(
        request: Request,
        exc: StarletteHTTPException,
    ) -> JSONResponse:
        if not _is_v1_path(request.url.path):
            return await http_exception_handler(request, exc)
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "schema_version": SCHEMA_VERSION,
                "error": {
                    "code": "INVALID_REQUEST",
                    "message": _bounded_utf8(exc.detail),
                    "details": {},
                },
            },
            headers=exc.headers,
        )

    @app.exception_handler(InteractiveSandboxError)
    async def interactive_error_handler(
        _request: Request,
        exc: InteractiveSandboxError,
    ) -> JSONResponse:
        error: dict[str, Any] = {
            "code": exc.error_code,
            "message": _bounded_utf8(exc),
            "details": exc.details,
        }
        request_id = getattr(exc, "request_id", None)
        session_id = getattr(exc, "session_id", None)
        workspace_session_id = getattr(exc, "workspace_session_id", None)
        if request_id is not None:
            error["request_id"] = request_id
        if session_id is not None:
            error["session_id"] = session_id
        if workspace_session_id is not None:
            error["workspace_session_id"] = workspace_session_id
        return JSONResponse(
            status_code=exc.status_code,
            content={"schema_version": SCHEMA_VERSION, "error": error},
        )

    @app.exception_handler(RequestValidationError)
    async def interactive_validation_handler(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        if not _is_v1_path(request.url.path):
            return await request_validation_exception_handler(request, exc)
        request_id: str | None = None
        if isinstance(exc.body, dict) and isinstance(exc.body.get("request_id"), str):
            request_id = _bounded_utf8(exc.body["request_id"], 256)
        error: dict[str, Any] = {
            "code": "INVALID_REQUEST",
            "message": "invalid interactive sandbox request",
            "details": {},
        }
        if request_id:
            error["request_id"] = request_id
        workspace_session_id = request.path_params.get("workspace_session_id")
        if workspace_session_id:
            error["workspace_session_id"] = str(workspace_session_id)
        return JSONResponse(
            status_code=400,
            content={"schema_version": SCHEMA_VERSION, "error": error},
        )
