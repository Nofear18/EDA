"""Create-idempotency and workspace-session queries for the registry."""

from __future__ import annotations

import time

from science_eda.exceptions import (
    CreateCapacityTimeoutError,
    InteractiveInvalidRequestError,
    InvalidWorkspacePathError,
    RequestIdConflictError,
)
from science_eda.sandbox.interactive.models import (
    CreateRequestClaim,
    CreateRequestRecord,
    CreateRequestState,
    RuntimeRecord,
    RuntimeState,
    SessionState,
    WorkspaceSessionRecord,
    timestamp_after,
    utc_now,
)
from science_eda.sandbox.interactive.registry_codec import (
    bounded_text,
    create_request_from_row,
    runtime_from_row,
    session_from_row,
    validate_request_id,
    validate_tool_kind,
)
from science_eda.sandbox.interactive.tool_config import validate_tool_version


class RegistryCreateMixin:
    """Persist create requests and publish/query their sessions."""

    def begin_create_request(
        self,
        request_id: str,
        tool_kind: str,
        workspace_path: str,
        *,
        version: str | None = None,
        now: str | None = None,
    ) -> CreateRequestClaim:
        validate_request_id(request_id)
        normalized_tool = validate_tool_kind(tool_kind)
        normalized_version = validate_tool_version(version)
        if not isinstance(workspace_path, str):
            raise InteractiveInvalidRequestError(
                "workspace_path must be a string",
                request_id=request_id,
            )
        try:
            workspace_path.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise InvalidWorkspacePathError(
                "workspace_path must be valid UTF-8 text",
                request_id=request_id,
            ) from exc
        timestamp = now or utc_now()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM create_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is not None:
                record = create_request_from_row(row)
                if (
                    record.tool_kind != normalized_tool
                    or record.version != normalized_version
                    or record.workspace_path_raw != workspace_path
                ):
                    raise RequestIdConflictError(
                        "create request_id was already used with different immutable fields",
                        request_id=request_id,
                    )
                return CreateRequestClaim(False, record)
            retain_until = timestamp_after(timestamp, self._create_retention)
            if not workspace_path or "\x00" in workspace_path:
                connection.execute(
                    """
                    INSERT INTO create_requests (
                        request_id, tool_kind, version, workspace_path_raw, state,
                        error_code, error_message, created_at, completed_at,
                        retain_until
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        request_id,
                        normalized_tool,
                        normalized_version,
                        workspace_path,
                        CreateRequestState.FAILED.value,
                        InvalidWorkspacePathError.error_code,
                        "workspace_path must be a non-empty absolute path",
                        timestamp,
                        timestamp,
                        retain_until,
                    ),
                )
                row = connection.execute(
                    "SELECT * FROM create_requests WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                # ``created`` remains true because this call won the durable
                # idempotency claim.  The service may submit its normal
                # background task, but the record is already terminal and no
                # runtime can be spawned for this syntactically invalid path.
                return CreateRequestClaim(True, create_request_from_row(row))
            pending_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM create_requests WHERE state = ?",
                    (CreateRequestState.PENDING.value,),
                ).fetchone()[0]
            )
            if pending_count >= self._max_sessions:
                # Keep accepted-but-not-yet-published create work bounded.  The
                # terminal row still preserves global request-id idempotency;
                # no background task is needed for this immediate outcome.
                connection.execute(
                    """
                    INSERT INTO create_requests (
                        request_id, tool_kind, version, workspace_path_raw, state,
                        error_code, error_message, created_at, completed_at,
                        retain_until
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        request_id,
                        normalized_tool,
                        normalized_version,
                        workspace_path,
                        CreateRequestState.FAILED.value,
                        CreateCapacityTimeoutError.error_code,
                        "interactive pending create limit reached",
                        timestamp,
                        timestamp,
                        retain_until,
                    ),
                )
                row = connection.execute(
                    "SELECT * FROM create_requests WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                return CreateRequestClaim(True, create_request_from_row(row))
            connection.execute(
                """
                INSERT INTO create_requests (
                    request_id, tool_kind, version, workspace_path_raw, state,
                    created_at, retain_until
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    normalized_tool,
                    normalized_version,
                    workspace_path,
                    CreateRequestState.PENDING.value,
                    timestamp,
                    retain_until,
                ),
            )
            row = connection.execute(
                "SELECT * FROM create_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            return CreateRequestClaim(True, create_request_from_row(row))

    def get_create_request(self, request_id: str) -> CreateRequestRecord | None:
        validate_request_id(request_id)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM create_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
        return None if row is None else create_request_from_row(row)

    def wait_for_create_terminal(
        self,
        request_id: str,
        *,
        timeout: float | None = None,
    ) -> CreateRequestRecord:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._terminal_condition:
            while True:
                record = self.get_create_request(request_id)
                if record is None:
                    raise InteractiveInvalidRequestError(
                        f"unknown create request_id: {request_id!r}",
                        request_id=request_id,
                    )
                if record.state is not CreateRequestState.PENDING:
                    return record
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return record
                self._terminal_condition.wait(remaining)

    def fail_create_request(
        self,
        request_id: str,
        error_code: str,
        error_message: str,
        *,
        canonical_workspace_path: str | None = None,
        now: str | None = None,
    ) -> CreateRequestRecord:
        timestamp = now or utc_now()
        with self._transaction() as connection:
            row = self._require_create(connection, request_id)
            if row["state"] != CreateRequestState.PENDING.value:
                return create_request_from_row(row)
            connection.execute(
                """
                UPDATE create_requests
                SET state = ?, canonical_workspace_path = ?, error_code = ?,
                    error_message = ?, completed_at = ?,
                    retain_until = MAX(retain_until, ?)
                WHERE request_id = ?
                """,
                (
                    CreateRequestState.FAILED.value,
                    canonical_workspace_path,
                    str(error_code),
                    bounded_text(error_message, self._preview_max),
                    timestamp,
                    timestamp_after(timestamp, self._create_retention),
                    request_id,
                ),
            )
            result = create_request_from_row(
                self._require_create(connection, request_id)
            )
        with self._terminal_condition:
            self._terminal_condition.notify_all()
        return result

    def publish_workspace_session(
        self,
        request_id: str,
        workspace_session_id: str,
        canonical_workspace_path: str,
        runtime_instance_id: str,
        process_id: int,
        scratch_relative_path: str,
        *,
        worker_process_id: int | None = None,
        worker_process_identity: str | None = None,
        process_identity: str | None = None,
        capacity_lease_id: str | None = None,
        now: str | None = None,
    ) -> tuple[WorkspaceSessionRecord, RuntimeRecord]:
        timestamp = now or utc_now()
        idle_expires = timestamp_after(timestamp, self._runtime_idle_ttl)
        with self._transaction() as connection:
            create_row = self._require_create(connection, request_id)
            if create_row["state"] == CreateRequestState.SUCCEEDED.value:
                session = self._require_session(
                    connection,
                    str(create_row["workspace_session_id"]),
                )
                runtime = self._require_runtime(
                    connection,
                    str(create_row["workspace_session_id"]),
                )
                return session_from_row(session), runtime_from_row(runtime)
            if create_row["state"] != CreateRequestState.PENDING.value:
                raise InteractiveInvalidRequestError(
                    "cannot publish a failed create request",
                    request_id=request_id,
                )
            active_count = connection.execute(
                "SELECT COUNT(*) FROM workspace_sessions WHERE state != ?",
                (SessionState.CLOSED.value,),
            ).fetchone()[0]
            if int(active_count) >= self._max_sessions:
                raise CreateCapacityTimeoutError(
                    "interactive workspace session limit reached",
                    request_id=request_id,
                )
            connection.execute(
                """
                INSERT INTO workspace_sessions (
                    workspace_session_id, tool_kind, version, workspace_path, state,
                    created_at, last_active_at, idle_expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    workspace_session_id,
                    create_row["tool_kind"],
                    create_row["version"],
                    canonical_workspace_path,
                    SessionState.ACTIVE.value,
                    timestamp,
                    timestamp,
                    idle_expires,
                ),
            )
            connection.execute(
                """
                INSERT INTO runtimes (
                    workspace_session_id, runtime_instance_id, tool_kind, state,
                    worker_process_id, worker_process_identity,
                    process_id, process_identity, scratch_relative_path,
                    capacity_lease_id, started_at, last_active_at, idle_expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    workspace_session_id,
                    runtime_instance_id,
                    create_row["tool_kind"],
                    RuntimeState.READY.value,
                    worker_process_id,
                    worker_process_identity,
                    int(process_id),
                    process_identity,
                    scratch_relative_path,
                    capacity_lease_id,
                    timestamp,
                    timestamp,
                    idle_expires,
                ),
            )
            connection.execute(
                """
                UPDATE create_requests
                SET state = ?, canonical_workspace_path = ?,
                    workspace_session_id = ?, completed_at = ?,
                    retain_until = MAX(retain_until, ?)
                WHERE request_id = ?
                """,
                (
                    CreateRequestState.SUCCEEDED.value,
                    canonical_workspace_path,
                    workspace_session_id,
                    timestamp,
                    timestamp_after(timestamp, self._create_retention),
                    request_id,
                ),
            )
            connection.execute(
                "DELETE FROM runtime_startups WHERE request_id = ?",
                (request_id,),
            )
            self._append_event(
                connection,
                workspace_session_id,
                "SESSION_CREATED",
                timestamp,
                transition={"session": {"from": None, "to": SessionState.ACTIVE.value}},
                runtime_instance_id=runtime_instance_id,
            )
            self._append_event(
                connection,
                workspace_session_id,
                "RUNTIME_STARTING",
                timestamp,
                runtime_instance_id=runtime_instance_id,
            )
            self._append_event(
                connection,
                workspace_session_id,
                "RUNTIME_READY",
                timestamp,
                transition={"runtime": {"from": None, "to": RuntimeState.READY.value}},
                runtime_instance_id=runtime_instance_id,
            )
            session = session_from_row(
                self._require_session(connection, workspace_session_id)
            )
            runtime = runtime_from_row(
                self._require_runtime(connection, workspace_session_id)
            )
        with self._terminal_condition:
            self._terminal_condition.notify_all()
        return session, runtime

    def get_workspace_session(self, workspace_session_id: str) -> WorkspaceSessionRecord:
        with self._connect() as connection:
            row = self._require_session(connection, workspace_session_id)
        return session_from_row(row)

    def get_runtime(self, workspace_session_id: str) -> RuntimeRecord:
        with self._connect() as connection:
            row = self._require_runtime(connection, workspace_session_id)
        return runtime_from_row(row)

    def get_workspace_session_bundle(
        self,
        workspace_session_id: str,
    ) -> tuple[WorkspaceSessionRecord, RuntimeRecord]:
        with self._connect() as connection:
            session = self._require_session(connection, workspace_session_id)
            runtime = self._require_runtime(connection, workspace_session_id)
        return session_from_row(session), runtime_from_row(runtime)

    def list_workspace_sessions(
        self,
        *,
        include_closed: bool = False,
    ) -> tuple[tuple[WorkspaceSessionRecord, RuntimeRecord], ...]:
        where = "" if include_closed else "WHERE s.state != ?"
        parameters: tuple[str, ...] = () if include_closed else (SessionState.CLOSED.value,)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT s.workspace_session_id
                FROM workspace_sessions AS s
                {where}
                ORDER BY s.created_at, s.workspace_session_id
                """,
                parameters,
            ).fetchall()
            return tuple(
                (
                    session_from_row(self._require_session(connection, row[0])),
                    runtime_from_row(self._require_runtime(connection, row[0])),
                )
                for row in rows
            )
