"""Row codecs, cursor tokens, and bounded value helpers for the registry."""

from __future__ import annotations

import base64
import binascii
import json
import sqlite3
from typing import Any

from science_eda.exceptions import (
    InteractiveInternalError,
    InteractiveInvalidRequestError,
    InvalidHistoryCursorError,
)
from science_eda.sandbox.interactive.models import (
    CreateRequestRecord,
    CreateRequestState,
    ExecutionRecord,
    ExecutionState,
    RuntimeRecord,
    RuntimeStartupRecord,
    RuntimeState,
    SessionEventRecord,
    SessionState,
    WorkspaceSessionRecord,
)
from science_eda.sandbox.interactive.registry_constants import SQLITE_INTEGER_MAX


def encode_history_cursor(workspace_session_id: str, sequence: int) -> str:
    if (
        isinstance(sequence, bool)
        or not isinstance(sequence, int)
        or sequence < 1
        or sequence > SQLITE_INTEGER_MAX
    ):
        raise ValueError("history cursor sequence is outside the SQLite integer range")
    payload = json.dumps(
        {"s": workspace_session_id, "q": sequence, "v": 1},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    token = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    return f"hcur_{token}"


def decode_history_cursor(cursor: str, workspace_session_id: str) -> int:
    try:
        if not isinstance(cursor, str) or not cursor.startswith("hcur_"):
            raise ValueError
        token = cursor.removeprefix("hcur_")
        if not token or len(token) > 2048:
            raise ValueError
        padding = "=" * (-len(token) % 4)
        raw = base64.b64decode(
            (token + padding).encode("ascii"),
            altchars=b"-_",
            validate=True,
        )
        canonical_token = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
        if canonical_token != token:
            raise ValueError
        payload = json.loads(raw)
        if (
            not isinstance(payload, dict)
            or set(payload) != {"s", "q", "v"}
            or type(payload["v"]) is not int
            or payload["v"] != 1
            or payload["s"] != workspace_session_id
            or isinstance(payload["q"], bool)
            or not isinstance(payload["q"], int)
            or payload["q"] < 1
            or payload["q"] > SQLITE_INTEGER_MAX
        ):
            raise ValueError
        canonical_payload = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        if raw != canonical_payload:
            raise ValueError
        return int(payload["q"])
    except (
        ValueError,
        TypeError,
        KeyError,
        UnicodeError,
        json.JSONDecodeError,
        binascii.Error,
    ) as exc:
        raise InvalidHistoryCursorError(
            "invalid history cursor",
            workspace_session_id=workspace_session_id,
        ) from exc


def create_request_from_row(row: sqlite3.Row) -> CreateRequestRecord:
    return CreateRequestRecord(
        request_id=str(row["request_id"]),
        tool_kind=str(row["tool_kind"]),
        version=row["version"],
        workspace_path_raw=str(row["workspace_path_raw"]),
        state=CreateRequestState(row["state"]),
        canonical_workspace_path=row["canonical_workspace_path"],
        workspace_session_id=row["workspace_session_id"],
        error_code=row["error_code"],
        error_message=row["error_message"],
        created_at=str(row["created_at"]),
        completed_at=row["completed_at"],
        retain_until=str(row["retain_until"]),
    )


def session_from_row(row: sqlite3.Row) -> WorkspaceSessionRecord:
    return WorkspaceSessionRecord(
        workspace_session_id=str(row["workspace_session_id"]),
        tool_kind=str(row["tool_kind"]),
        version=row["version"],
        workspace_path=str(row["workspace_path"]),
        state=SessionState(row["state"]),
        created_at=str(row["created_at"]),
        last_active_at=str(row["last_active_at"]),
        idle_expires_at=row["idle_expires_at"],
        closed_at=row["closed_at"],
        audit_expires_at=row["audit_expires_at"],
        close_result=load_json(row["close_result_json"]),
        next_sequence=int(row["next_sequence"]),
        log_quota_used_bytes=int(row["log_quota_used_bytes"]),
    )


def runtime_from_row(row: sqlite3.Row) -> RuntimeRecord:
    return RuntimeRecord(
        workspace_session_id=str(row["workspace_session_id"]),
        runtime_instance_id=str(row["runtime_instance_id"]),
        tool_kind=str(row["tool_kind"]),
        state=RuntimeState(row["state"]),
        worker_process_id=(
            None
            if row["worker_process_id"] is None
            else int(row["worker_process_id"])
        ),
        worker_process_identity=row["worker_process_identity"],
        process_id=None if row["process_id"] is None else int(row["process_id"]),
        process_identity=row["process_identity"],
        scratch_relative_path=str(row["scratch_relative_path"]),
        capacity_lease_id=row["capacity_lease_id"],
        current_execution_id=row["current_execution_id"],
        lost_reason=row["lost_reason"],
        lost_at=row["lost_at"],
        started_at=str(row["started_at"]),
        last_active_at=str(row["last_active_at"]),
        idle_expires_at=row["idle_expires_at"],
        stopped_at=row["stopped_at"],
    )


def runtime_startup_from_row(row: sqlite3.Row) -> RuntimeStartupRecord:
    return RuntimeStartupRecord(
        request_id=str(row["request_id"]),
        workspace_session_id=str(row["workspace_session_id"]),
        tool_kind=str(row["tool_kind"]),
        scratch_relative_path=str(row["scratch_relative_path"]),
        spawn_attempted=bool(row["spawn_attempted"]),
        worker_process_id=(
            None
            if row["worker_process_id"] is None
            else int(row["worker_process_id"])
        ),
        worker_process_identity=row["worker_process_identity"],
        process_id=None if row["process_id"] is None else int(row["process_id"]),
        process_identity=row["process_identity"],
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def execution_from_row(row: sqlite3.Row) -> ExecutionRecord:
    return ExecutionRecord(
        execution_id=str(row["execution_id"]),
        request_id=str(row["request_id"]),
        workspace_session_id=str(row["workspace_session_id"]),
        sequence=int(row["sequence"]),
        tool_kind=str(row["tool_kind"]),
        runtime_instance_id=str(row["runtime_instance_id"]),
        code=str(row["code"]),
        code_bytes=int(row["code_bytes"]),
        code_sha256=str(row["code_sha256"]),
        timeout_ms=int(row["timeout_ms"]),
        state=ExecutionState(row["state"]),
        submitted_at=str(row["submitted_at"]),
        started_at=row["started_at"],
        ended_at=row["ended_at"],
        exit_code=None if row["exit_code"] is None else int(row["exit_code"]),
        error=load_json(row["error_json"]),
        output_preview=row["output_preview"],
        preview_strategy=row["preview_strategy"],
        returned_bytes=optional_int(row["returned_bytes"]),
        output_bytes=optional_int(row["output_bytes"]),
        output_lines=optional_int(row["output_lines"]),
        output_truncated=from_db_bool(row["output_truncated"]),
        full_log_ref=str(row["full_log_ref"]),
        local_log_path=str(row["local_log_path"]),
        log_quota_reserved_bytes=int(row["log_quota_reserved_bytes"]),
        log_quota_accounted_bytes=int(row["log_quota_accounted_bytes"]),
        full_log_complete=from_db_bool(row["full_log_complete"]),
        full_log_written_bytes=optional_int(row["full_log_written_bytes"]),
        full_log_dropped_bytes=optional_int(row["full_log_dropped_bytes"]),
        full_log_incomplete_reason=row["full_log_incomplete_reason"],
        runtime_preserved=from_db_bool(row["runtime_preserved"]),
        log_retained=bool(row["log_retained"]),
    )


def event_from_row(row: sqlite3.Row) -> SessionEventRecord:
    return SessionEventRecord(
        event_id=str(row["event_id"]),
        workspace_session_id=str(row["workspace_session_id"]),
        occurred_at=str(row["occurred_at"]),
        event_type=str(row["event_type"]),
        actor_kind=str(row["actor_kind"]),
        actor_id=str(row["actor_id"]),
        transition=load_json(row["transition_json"]),
        runtime_instance_id=row["runtime_instance_id"],
        execution_id=row["execution_id"],
        reason=load_json(row["reason_json"]),
    )


def validate_request_id(request_id: str) -> None:
    encoded: bytes | None = None
    if isinstance(request_id, str):
        try:
            encoded = request_id.encode("utf-8")
        except UnicodeEncodeError:
            pass
    if (
        not isinstance(request_id, str)
        or not request_id
        or "\x00" in request_id
        or encoded is None
        or len(encoded) > 256
    ):
        raise InteractiveInvalidRequestError(
            "request_id must contain 1 to 256 UTF-8 bytes",
            request_id=request_id if encoded is not None else None,
        )


def validate_tool_kind(tool_kind: str) -> str:
    if not isinstance(tool_kind, str):
        raise InteractiveInvalidRequestError("tool_kind must be a string")
    normalized = tool_kind.strip().lower()
    if normalized not in {"innovus", "primetime"}:
        raise InteractiveInvalidRequestError(
            f"unsupported tool_kind: {tool_kind!r}",
            details={"supported_tool_kinds": ["innovus", "primetime"]},
        )
    return normalized


def dump_json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def load_json(value: str | None) -> Any:
    return None if value is None else json.loads(value)


def bounded_text(value: object, max_bytes: int) -> str:
    encoded = str(value).encode("utf-8", errors="replace")
    if len(encoded) <= max_bytes:
        return encoded.decode("utf-8")
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def bounded_error(value: dict[str, Any] | None, max_bytes: int) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "code": bounded_text(value.get("code", "EXECUTION_ERROR"), 256),
        "message": bounded_text(value.get("message", ""), max_bytes),
    }


def bounded_json(value: Any, max_bytes: int) -> str | None:
    if value is None:
        return None
    encoded = dump_json(value)
    assert encoded is not None
    if len(encoded.encode("utf-8")) <= max_bytes:
        return encoded
    return dump_json({"truncated": True, "summary": bounded_text(value, max_bytes // 2)})


def optional_nonnegative(value: int | None, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise InteractiveInternalError(f"{name} must be a non-negative integer")
    return value


def optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def to_db_bool(value: bool | None) -> int | None:
    return None if value is None else int(bool(value))


def from_db_bool(value: Any) -> bool | None:
    return None if value is None else bool(value)
