"""HTTP Sandbox server (FastAPI)."""

from __future__ import annotations

from contextlib import asynccontextmanager
from email import policy
from email.parser import BytesParser
from typing import Any, Optional

from science_eda.config import SandboxConfig, load_config_mapping, merge_dataclass_config
from science_eda.dataset.innovus_grpo import INNOVUS_GRPO_DATASET_DIR
from science_eda.exceptions import (
    ExecutionError,
    LanguageMismatchError,
    SandboxPathError,
    SessionAlreadyExistsError,
    SessionNotFoundError,
)
from science_eda.exceptions import TimeoutError as ExecutionTimeoutError
from science_eda.sandbox.file_io import (
    SnapshotCache,
    read_text_file,
    stage_snapshot_from_cache,
    write_text_file,
    write_upload,
)
from science_eda.sandbox.interactive.http_api import (
    build_interactive_router,
    install_interactive_error_handlers,
)
from science_eda.sandbox.interactive.service import InteractiveService
from science_eda.sandbox.session import SessionManager
from science_eda.sandbox.types import ExecutionResult

try:
    from fastapi import FastAPI, Request
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel, Field
except ImportError as e:  # pragma: no cover
    raise ImportError(
        "Sandbox HTTP server requires 'fastapi' and 'pydantic'. "
        "Install with: pip install fastapi uvicorn pydantic"
    ) from e


class CreateSessionBody(BaseModel):
    lang: str
    session_id: Optional[str] = None


class RunInSessionBody(BaseModel):
    session_id: str
    code: str
    lang: Optional[str] = None
    timeout: Optional[float] = Field(default=None, description="Override default execute timeout")


class ExecuteBody(BaseModel):
    code: str
    lang: str
    timeout: Optional[float] = Field(default=None, description="Override default execute timeout")


class CloseSessionBody(BaseModel):
    session_id: str


class ReadFileBody(BaseModel):
    session_id: str
    path: str
    encoding: str = "utf-8"
    errors: str = "strict"


class WriteFileBody(BaseModel):
    session_id: str
    path: str
    content: str
    encoding: str = "utf-8"


class StageSnapshotBody(BaseModel):
    session_id: str
    snapshot_ref: str
    target_path: str


def create_app(config: SandboxConfig | None = None) -> FastAPI:
    cfg = config or SandboxConfig()
    cfg.validate_interactive()
    manager = SessionManager(cfg) if cfg.legacy_batch_enabled else None
    interactive_service = InteractiveService(cfg) if cfg.interactive_enabled else None
    snapshot_cache = None
    if manager is not None:
        snapshot_cache_root = (
            cfg.innovus_snapshot_cache_root
            or str(INNOVUS_GRPO_DATASET_DIR / "snapshots")
        )
        snapshot_cache = SnapshotCache(snapshot_cache_root)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        _configure_http_thread_limit(cfg)
        if manager is not None:
            manager.start_pool_supervisors()
        try:
            yield
        finally:
            try:
                if manager is not None:
                    manager.close_all()
            finally:
                if interactive_service is not None:
                    interactive_service.shutdown()

    app = FastAPI(title="science_eda Sandbox", version="0.3.0", lifespan=lifespan)

    def _execution_timeout(timeout: float | None) -> float:
        return float(timeout) if timeout is not None else float(cfg.timeout)

    def _to_json(res: ExecutionResult) -> dict[str, Any]:
        return {
            "stdout": res.stdout,
            "stderr": res.stderr,
            "exit_code": res.exit_code,
            "duration": res.duration,
            "metadata": res.metadata,
        }

    def _error_response(status_code: int, code: str, message: str) -> JSONResponse:
        return JSONResponse(
            status_code=status_code,
            content={"error": {"code": code, "message": message}},
        )

    @app.exception_handler(SessionNotFoundError)
    async def session_not_found(_: Request, exc: SessionNotFoundError) -> JSONResponse:
        return _error_response(404, "session_not_found", str(exc))

    @app.exception_handler(SessionAlreadyExistsError)
    async def session_already_exists(
        _: Request, exc: SessionAlreadyExistsError
    ) -> JSONResponse:
        return _error_response(409, "session_already_exists", str(exc))

    @app.exception_handler(LanguageMismatchError)
    async def language_mismatch(_: Request, exc: LanguageMismatchError) -> JSONResponse:
        return _error_response(400, "language_mismatch", str(exc))

    @app.exception_handler(SandboxPathError)
    async def invalid_path(_: Request, exc: SandboxPathError) -> JSONResponse:
        return _error_response(400, "invalid_path", str(exc))

    @app.exception_handler(FileNotFoundError)
    async def file_not_found(_: Request, exc: FileNotFoundError) -> JSONResponse:
        return _error_response(404, "file_not_found", str(exc))

    @app.exception_handler(ValueError)
    async def invalid_request(_: Request, exc: ValueError) -> JSONResponse:
        return _error_response(400, "invalid_request", str(exc))

    @app.exception_handler(ExecutionTimeoutError)
    async def timeout_error(_: Request, exc: ExecutionTimeoutError) -> JSONResponse:
        return _error_response(504, "timeout", str(exc))

    @app.exception_handler(ExecutionError)
    async def execution_error(_: Request, exc: ExecutionError) -> JSONResponse:
        return _error_response(500, "execution_error", str(exc))

    @app.get("/")
    def root() -> dict[str, str]:
        return {"message": "science_eda sandbox"}

    @app.get("/is_alive")
    def is_alive() -> dict[str, Any]:
        return {"is_alive": True, "message": ""}

    if manager is not None:
        legacy_manager = manager
        legacy_snapshot_cache = snapshot_cache

        @app.get("/pool_state")
        def pool_state() -> dict[str, Any]:
            return {
                "innovus": legacy_manager.innovus_pool_state(),
                "primetime": legacy_manager.primetime_pool_state(),
            }

        @app.post("/create_session")
        def create_session(body: CreateSessionBody) -> dict[str, Any]:
            sid = legacy_manager.create_session(
                {"lang": body.lang, "session_id": body.session_id}
            )
            return {"session_id": sid}

        @app.post("/run_in_session")
        def run_in_session(body: RunInSessionBody) -> dict[str, Any]:
            sess = legacy_manager.get_session(body.session_id)
            lang = body.lang if body.lang is not None else sess.lang
            res = legacy_manager.execute_in_session(
                body.session_id,
                body.code,
                lang,
                timeout=_execution_timeout(body.timeout),
            )
            return _to_json(res)

        @app.post("/execute")
        def execute(body: ExecuteBody) -> dict[str, Any]:
            sid = legacy_manager.create_session({"lang": body.lang})
            try:
                res = legacy_manager.execute_in_session(
                    sid,
                    body.code,
                    body.lang,
                    timeout=_execution_timeout(body.timeout),
                )
                return _to_json(res)
            finally:
                try:
                    legacy_manager.close_session(sid)
                except SessionNotFoundError:
                    pass

        @app.post("/close_session")
        def close_session(body: CloseSessionBody) -> dict[str, str]:
            legacy_manager.close_session(body.session_id)
            return {"status": "ok"}

        @app.post("/read_file")
        def read_file(body: ReadFileBody) -> dict[str, str]:
            with legacy_manager.locked_session(body.session_id) as sess:
                content = read_text_file(
                    sess.working_dir,
                    body.path,
                    encoding=body.encoding,
                    errors=body.errors,
                )
            return {"content": content}

        @app.post("/write_file")
        def write_file(body: WriteFileBody) -> dict[str, str]:
            with legacy_manager.locked_session(body.session_id) as sess:
                write_text_file(
                    sess.working_dir,
                    body.path,
                    body.content,
                    encoding=body.encoding,
                )
            return {"status": "ok"}

        @app.post("/stage_snapshot")
        def stage_snapshot(body: StageSnapshotBody) -> dict[str, str]:
            assert legacy_snapshot_cache is not None
            with legacy_manager.locked_session(body.session_id) as sess:
                snapshot_path = stage_snapshot_from_cache(
                    sess.working_dir,
                    legacy_snapshot_cache,
                    body.snapshot_ref,
                    body.target_path,
                )
            return {"status": "ok", "snapshot_path": snapshot_path}

        @app.post("/upload")
        async def upload(request: Request) -> dict[str, str]:
            fields, file_content = await _parse_upload_request(request)
            session_id = _required_field(fields, "session_id")
            target_path = _required_field(fields, "target_path")
            unzip = _parse_bool(fields.get("unzip", "false"))
            with legacy_manager.locked_session(session_id) as sess:
                write_upload(sess.working_dir, target_path, file_content, unzip=unzip)
            return {"status": "ok"}

    if interactive_service is not None:
        app.include_router(build_interactive_router(interactive_service, cfg))
        install_interactive_error_handlers(app)
        app.state.interactive_service = interactive_service

    return app


def _http_thread_limit(config: SandboxConfig) -> int:
    explicit = int(config.http_thread_limit)
    if explicit > 0:
        return explicit
    tool_capacity = 0
    if config.legacy_batch_enabled:
        tool_capacity += int(config.innovus_pool_size)
        if config.primetime_use_pool:
            tool_capacity += int(config.primetime_pool_size)
    if config.interactive_enabled:
        tool_capacity += int(config.interactive_innovus_capacity)
        tool_capacity += int(config.interactive_primetime_capacity)
    return tool_capacity * 4 + 32


def _configure_http_thread_limit(config: SandboxConfig) -> None:
    import anyio.to_thread

    anyio.to_thread.current_default_thread_limiter().total_tokens = _http_thread_limit(
        config,
    )


def _required_field(fields: dict[str, str], name: str) -> str:
    value = fields.get(name)
    if value is None or value == "":
        raise ValueError(f"missing upload field: {name}")
    return value


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


async def _parse_upload_request(request: Request) -> tuple[dict[str, str], bytes]:
    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" not in content_type:
        raise ValueError("upload requires multipart/form-data")
    body = await request.body()
    message = BytesParser(policy=policy.default).parsebytes(
        f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode("utf-8")
        + body
    )
    if not message.is_multipart():
        raise ValueError("upload request is not multipart")

    fields: dict[str, str] = {}
    file_content: bytes | None = None
    for part in message.iter_parts():
        name = part.get_param("name", header="content-disposition")
        if not name:
            continue
        payload = part.get_payload(decode=True) or b""
        if name == "file":
            file_content = payload
        else:
            fields[str(name)] = payload.decode("utf-8")
    if file_content is None:
        raise ValueError("missing upload field: file")
    return fields, file_content


def _load_server_config(config_path: str | None) -> SandboxConfig:
    if not config_path:
        return SandboxConfig()
    raw = load_config_mapping(config_path)
    sandbox_raw = raw.get("sandbox", {})
    if not isinstance(sandbox_raw, dict):
        raise ValueError("Config file must contain a mapping/object sandbox section.")
    return merge_dataclass_config(SandboxConfig(), sandbox_raw)


def main(argv: list[str] | None = None) -> None:
    import argparse
    import uvicorn

    parser = argparse.ArgumentParser(description="Run the science_eda sandbox HTTP server.")
    parser.add_argument(
        "-c",
        "--config",
        help="Path to a science_eda YAML/TOML config file. Defaults to SandboxConfig().",
    )
    args = parser.parse_args(argv)

    cfg = _load_server_config(args.config)
    app = create_app(cfg)
    uvicorn.run(app, host=cfg.server_host, port=int(cfg.server_port))


if __name__ == "__main__":  # pragma: no cover
    main()
