"""Stable HTTP representations for durable interactive records."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from science_eda.config import SandboxConfig
from science_eda.sandbox.interactive.models import (
    SCHEMA_VERSION,
    ExecutionRecord,
    RuntimeRecord,
    WorkspaceSessionRecord,
)
from science_eda.sandbox.interactive.tool_config import SUPPORTED_TOOL_KINDS
from science_eda.sandbox.interactive.workspace import InteractivePaths


def capabilities_payload(
    config: SandboxConfig,
    paths: InteractivePaths,
) -> dict[str, Any]:
    return {
        "deployment_mode": "trusted_local",
        "shared_filesystem": True,
        "local_log_root": str(paths.log_root),
        "workspace_path_policy": {
            "required": True,
            "kind": "local_absolute_directory",
            "canonicalization": "realpath",
            "runtime_initial_cwd": True,
            "managed_root_overlap": "reject",
            "filesystem_scope": "host_user",
        },
        "supported_tool_kinds": list(SUPPORTED_TOOL_KINDS),
        "limits": {
            "max_code_bytes": config.interactive_max_code_bytes,
            "output_preview_max_bytes": config.interactive_output_preview_max_bytes,
            "execution_log_max_bytes": config.interactive_execution_log_max_bytes,
            "session_log_max_bytes": config.interactive_session_log_max_bytes,
            "create_request_retention_seconds": (
                config.interactive_create_request_retention_ttl
            ),
            "history_default_page_size": config.interactive_history_default_page_size,
            "history_max_page_size": config.interactive_history_max_page_size,
            "history_page_max_bytes": config.interactive_history_page_max_bytes,
        },
    }


def workspace_session_payload(
    session: WorkspaceSessionRecord,
    runtime: RuntimeRecord,
) -> dict[str, Any]:
    public_process_id = (
        runtime.process_id
        if runtime.state.value in {"READY", "BUSY"}
        else None
    )
    return {
        "workspace_session_id": session.workspace_session_id,
        "tool_kind": session.tool_kind,
        "version": session.version,
        "workspace_path": session.workspace_path,
        "state": session.state.value,
        "runtime": {
            "state": runtime.state.value,
            "runtime_instance_id": runtime.runtime_instance_id,
            "process_id": public_process_id,
            "current_execution_id": runtime.current_execution_id,
            "lost_reason": runtime.lost_reason,
            "lost_at": runtime.lost_at,
        },
        "timestamps": {
            "created_at": session.created_at,
            "last_active_at": session.last_active_at,
            "idle_expires_at": runtime.idle_expires_at,
        },
    }


def workspace_session_summary(
    session: WorkspaceSessionRecord,
    runtime: RuntimeRecord,
) -> dict[str, Any]:
    return {
        "workspace_session_id": session.workspace_session_id,
        "tool_kind": session.tool_kind,
        "version": session.version,
        "workspace_path": session.workspace_path,
        "state": session.state.value,
        "runtime_state": runtime.state.value,
        "current_execution_id": runtime.current_execution_id,
        "created_at": session.created_at,
        "last_active_at": session.last_active_at,
        "idle_expires_at": runtime.idle_expires_at,
    }


def full_log_payload(execution: ExecutionRecord) -> dict[str, Any]:
    return {
        "ref": execution.full_log_ref,
        "access": "local_file",
        "path": execution.local_log_path,
        "complete": execution.full_log_complete,
        "written_bytes": execution.full_log_written_bytes,
        "dropped_bytes": execution.full_log_dropped_bytes,
        "incomplete_reason": execution.full_log_incomplete_reason,
    }


def execution_payload(
    execution: ExecutionRecord,
    runtime: RuntimeRecord,
) -> dict[str, Any]:
    output = {
        "preview": execution.output_preview,
        "preview_strategy": execution.preview_strategy,
        "returned_bytes": execution.returned_bytes,
        "total_bytes": execution.output_bytes,
        "total_lines": execution.output_lines,
        "truncated": execution.output_truncated,
        "full_log": full_log_payload(execution),
    }
    public_state = "RUNNING" if execution.state.value == "QUEUED" else execution.state.value
    return {
        "execution_id": execution.execution_id,
        "request_id": execution.request_id,
        "workspace_session_id": execution.workspace_session_id,
        "tool_kind": execution.tool_kind,
        "state": public_state,
        "timing": {
            "submitted_at": execution.submitted_at,
            "started_at": execution.started_at,
            "ended_at": execution.ended_at,
            "duration_ms": execution.duration_ms,
        },
        "result": {
            "exit_code": execution.exit_code,
            "error": execution.error,
            "output": output,
        },
        "runtime": {
            "state": runtime.state.value,
            "runtime_instance_id": runtime.runtime_instance_id,
            "preserved": execution.runtime_preserved,
            "lost_reason": runtime.lost_reason,
            "lost_at": runtime.lost_at,
        },
    }


def history_item_payload(execution: ExecutionRecord) -> dict[str, Any]:
    public_state = "RUNNING" if execution.state.value == "QUEUED" else execution.state.value
    return {
        "sequence": execution.sequence,
        "execution_id": execution.execution_id,
        "request_id": execution.request_id,
        "state": public_state,
        "code": execution.code,
        "timing": {
            "submitted_at": execution.submitted_at,
            "started_at": execution.started_at,
            "ended_at": execution.ended_at,
            "duration_ms": execution.duration_ms,
        },
        "result_summary": {
            "exit_code": execution.exit_code,
            "error": execution.error,
            "output": {
                "preview": execution.output_preview,
                "total_bytes": execution.output_bytes,
                "total_lines": execution.output_lines,
                "truncated": execution.output_truncated,
                "full_log": full_log_payload(execution),
            },
        },
    }


def history_page_payload(
    workspace_session_id: str,
    executions: Sequence[ExecutionRecord],
    *,
    has_more: bool,
    next_cursor: str | None,
    include_schema_version: bool = True,
) -> dict[str, Any]:
    """Build the exact public history envelope used for byte accounting."""

    payload: dict[str, Any] = {
        "workspace_session_id": workspace_session_id,
        "order": "sequence_asc",
        "history": [history_item_payload(item) for item in executions],
        "page": {
            "count": len(executions),
            "has_more": has_more,
            "next_cursor": next_cursor,
        },
    }
    if include_schema_version:
        # ``_with_schema`` adds this field last at the HTTP boundary.  Keeping
        # the same shape here makes the byte count match Starlette's response.
        payload["schema_version"] = SCHEMA_VERSION
    return payload


def json_response_size(payload: dict[str, Any]) -> int:
    """Return bytes emitted by Starlette's compact UTF-8 JSON encoding."""

    return len(
        json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )
