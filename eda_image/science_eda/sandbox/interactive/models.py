"""Durable domain records for the interactive sandbox control plane."""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any


SCHEMA_VERSION = "1"


class CreateRequestState(str, Enum):
    PENDING = "PENDING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class SessionState(str, Enum):
    ACTIVE = "ACTIVE"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"


class RuntimeState(str, Enum):
    STOPPED = "STOPPED"
    READY = "READY"
    BUSY = "BUSY"
    LOST = "LOST"


class ExecutionState(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    LOST = "LOST"

    @property
    def terminal(self) -> bool:
        return self in {
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.TIMED_OUT,
            ExecutionState.LOST,
        }


@dataclass(frozen=True)
class CreateRequestRecord:
    request_id: str
    tool_kind: str
    version: str | None
    workspace_path_raw: str
    state: CreateRequestState
    canonical_workspace_path: str | None
    workspace_session_id: str | None
    error_code: str | None
    error_message: str | None
    created_at: str
    completed_at: str | None
    retain_until: str


@dataclass(frozen=True)
class WorkspaceSessionRecord:
    workspace_session_id: str
    tool_kind: str
    version: str | None
    workspace_path: str
    state: SessionState
    created_at: str
    last_active_at: str
    idle_expires_at: str | None
    closed_at: str | None
    audit_expires_at: str | None
    close_result: dict[str, Any] | None
    next_sequence: int
    log_quota_used_bytes: int


@dataclass(frozen=True)
class RuntimeRecord:
    workspace_session_id: str
    runtime_instance_id: str
    tool_kind: str
    state: RuntimeState
    worker_process_id: int | None
    worker_process_identity: str | None
    process_id: int | None
    process_identity: str | None
    scratch_relative_path: str
    capacity_lease_id: str | None
    current_execution_id: str | None
    lost_reason: str | None
    lost_at: str | None
    started_at: str
    last_active_at: str
    idle_expires_at: str | None
    stopped_at: str | None


@dataclass(frozen=True)
class ExecutionRecord:
    execution_id: str
    request_id: str
    workspace_session_id: str
    sequence: int
    tool_kind: str
    runtime_instance_id: str
    code: str
    code_bytes: int
    code_sha256: str
    timeout_ms: int
    state: ExecutionState
    submitted_at: str
    started_at: str | None
    ended_at: str | None
    exit_code: int | None
    error: dict[str, Any] | None
    output_preview: str | None
    preview_strategy: str | None
    returned_bytes: int | None
    output_bytes: int | None
    output_lines: int | None
    output_truncated: bool | None
    full_log_ref: str
    local_log_path: str
    log_quota_reserved_bytes: int
    log_quota_accounted_bytes: int
    full_log_complete: bool | None
    full_log_written_bytes: int | None
    full_log_dropped_bytes: int | None
    full_log_incomplete_reason: str | None
    runtime_preserved: bool | None
    log_retained: bool

    @property
    def duration_ms(self) -> int | None:
        if self.started_at is None or self.ended_at is None:
            return None
        start = parse_timestamp(self.started_at)
        end = parse_timestamp(self.ended_at)
        return max(0, round((end - start).total_seconds() * 1000))


@dataclass(frozen=True)
class SessionEventRecord:
    event_id: str
    workspace_session_id: str
    occurred_at: str
    event_type: str
    actor_kind: str
    actor_id: str
    transition: dict[str, Any] | None
    runtime_instance_id: str | None
    execution_id: str | None
    reason: dict[str, Any] | None


@dataclass(frozen=True)
class RuntimeStartupRecord:
    """Durable ownership published before a create task can spawn a process."""

    request_id: str
    workspace_session_id: str
    tool_kind: str
    scratch_relative_path: str
    spawn_attempted: bool
    worker_process_id: int | None
    worker_process_identity: str | None
    process_id: int | None
    process_identity: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class OrphanRuntimeRecord:
    """Process identity which a restarted daemon must quarantine."""

    tool_kind: str
    scratch_relative_path: str
    worker_process_id: int | None
    process_id: int | None
    worker_process_identity: str | None = None
    process_identity: str | None = None
    ownership_unknown: bool = False
    request_id: str | None = None
    workspace_session_id: str | None = None


@dataclass(frozen=True)
class CreateRequestClaim:
    created: bool
    record: CreateRequestRecord


@dataclass(frozen=True)
class ExecutionClaim:
    created: bool
    record: ExecutionRecord


@dataclass(frozen=True)
class HistoryPage:
    workspace_session_id: str
    records: tuple[ExecutionRecord, ...]
    has_more: bool
    next_cursor: str | None
    estimated_bytes: int


@dataclass(frozen=True)
class ReconciliationResult:
    pending_creates_failed: int
    executions_lost: int
    runtimes_lost: int
    scratch_relative_paths: tuple[str, ...]
    orphan_runtimes: tuple[OrphanRuntimeRecord, ...] = ()


@dataclass(frozen=True)
class GarbageCollectionResult:
    create_request_ids: tuple[str, ...]
    workspace_session_ids: tuple[str, ...]
    scratch_relative_paths: tuple[str, ...]
    local_log_paths: tuple[str, ...]


def utc_now() -> str:
    """Return a stable millisecond-resolution UTC timestamp."""

    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def timestamp_after(value: str, seconds: float) -> str:
    timestamp = parse_timestamp(value).timestamp() + seconds
    return (
        datetime.fromtimestamp(timestamp, timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def new_id(prefix: str) -> str:
    """Generate a service-owned opaque identifier without caller input."""

    return f"{prefix}_{uuid.uuid4().hex}"


def record_to_dict(record: object) -> dict[str, Any]:
    """Convert a record to JSON-compatible primitives for API composition."""

    def convert(value: Any) -> Any:
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, dict):
            return {str(key): convert(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [convert(item) for item in value]
        return value

    return convert(asdict(record))
