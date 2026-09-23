"""Execution reservation, transitions, history, and log accounting."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from typing import Any

from science_eda.exceptions import (
    InteractiveCodeTooLargeError,
    InteractiveInternalError,
    InteractiveInvalidRequestError,
    InteractiveRuntimeLostError,
    InteractiveRuntimeStoppedError,
    InteractiveSessionBusyError,
    RequestIdConflictError,
)
from science_eda.sandbox.interactive.models import (
    ExecutionClaim,
    ExecutionRecord,
    ExecutionState,
    HistoryPage,
    RuntimeState,
    SessionState,
    timestamp_after,
    utc_now,
)
from science_eda.sandbox.interactive.registry_codec import (
    bounded_error,
    bounded_text,
    decode_history_cursor,
    dump_json,
    encode_history_cursor,
    execution_from_row,
    optional_nonnegative,
    to_db_bool,
    validate_request_id,
)
from science_eda.sandbox.interactive.registry_constants import SQLITE_INTEGER_MAX
from science_eda.sandbox.interactive.serialization import (
    history_page_payload,
    json_response_size,
)


def _worst_case_history_record(
    record: ExecutionRecord,
    bounded_text_bytes: int,
) -> ExecutionRecord:
    """Return a conservative terminal form for single-record admission."""

    worst_text = "\x00" * bounded_text_bytes
    return replace(
        record,
        state=ExecutionState.FAILED,
        started_at=record.submitted_at,
        ended_at=record.submitted_at,
        exit_code=-2_147_483_648,
        error={"code": "\x00" * 256, "message": worst_text},
        output_preview=worst_text,
        output_bytes=9_223_372_036_854_775_807,
        output_lines=9_223_372_036_854_775_807,
        output_truncated=True,
        full_log_complete=False,
        full_log_written_bytes=record.log_quota_reserved_bytes,
        full_log_dropped_bytes=9_223_372_036_854_775_807,
        full_log_incomplete_reason=worst_text,
        runtime_preserved=False,
    )


class RegistryExecutionMixin:
    """Persist the execution state machine and its bounded result data."""

    def begin_execution(
        self,
        workspace_session_id: str,
        request_id: str,
        code: str,
        timeout_ms: int,
        *,
        execution_id: str,
        full_log_ref: str,
        local_log_path: str,
        log_quota_reserved_bytes: int | None = None,
        now: str | None = None,
    ) -> ExecutionClaim:
        validate_request_id(request_id)
        timestamp = now or utc_now()
        with self._transaction() as connection:
            session_row = self._require_session(connection, workspace_session_id)
            existing = connection.execute(
                """
                SELECT * FROM executions
                WHERE workspace_session_id = ? AND request_id = ?
                """,
                (workspace_session_id, request_id),
            ).fetchone()
            if existing is not None:
                record = execution_from_row(existing)
                if (
                    not isinstance(code, str)
                    or isinstance(timeout_ms, bool)
                    or not isinstance(timeout_ms, int)
                    or record.code != code
                    or record.timeout_ms != timeout_ms
                ):
                    raise RequestIdConflictError(
                        "execute request_id was already used with different immutable fields",
                        request_id=request_id,
                        workspace_session_id=workspace_session_id,
                    )
                return ExecutionClaim(False, record)

            if not isinstance(code, str):
                raise InteractiveInvalidRequestError(
                    "code must be a string",
                    request_id=request_id,
                    workspace_session_id=workspace_session_id,
                )
            try:
                encoded = code.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise InteractiveInvalidRequestError(
                    "code must be valid UTF-8 text",
                    request_id=request_id,
                    workspace_session_id=workspace_session_id,
                ) from exc
            if len(encoded) > self._max_code_bytes:
                raise InteractiveCodeTooLargeError(
                    f"code exceeds {self._max_code_bytes} UTF-8 bytes",
                    request_id=request_id,
                    workspace_session_id=workspace_session_id,
                    details={
                        "max_code_bytes": self._max_code_bytes,
                        "actual_code_bytes": len(encoded),
                    },
                )
            if (
                isinstance(timeout_ms, bool)
                or not isinstance(timeout_ms, int)
                or timeout_ms <= 0
                or timeout_ms > SQLITE_INTEGER_MAX
            ):
                raise InteractiveInvalidRequestError(
                    f"timeout_ms must be an integer from 1 to {SQLITE_INTEGER_MAX}",
                    request_id=request_id,
                    workspace_session_id=workspace_session_id,
                )
            requested_reservation = (
                self._execution_log_max_bytes
                if log_quota_reserved_bytes is None
                else log_quota_reserved_bytes
            )
            if (
                isinstance(requested_reservation, bool)
                or not isinstance(requested_reservation, int)
                or requested_reservation <= 0
            ):
                raise InteractiveInvalidRequestError(
                    "log quota reservation must be positive"
                )
            if requested_reservation > self._execution_log_max_bytes:
                raise InteractiveInvalidRequestError(
                    "log quota reservation exceeds the per-execution limit"
                )
            if session_row["state"] != SessionState.ACTIVE.value:
                raise InteractiveRuntimeStoppedError(
                    "workspace session is closed",
                    request_id=request_id,
                    workspace_session_id=workspace_session_id,
                )
            runtime_row = self._require_runtime(connection, workspace_session_id)
            runtime_state = RuntimeState(runtime_row["state"])
            if runtime_state is RuntimeState.LOST:
                raise InteractiveRuntimeLostError(
                    "interactive runtime is lost",
                    request_id=request_id,
                    workspace_session_id=workspace_session_id,
                )
            if runtime_state is RuntimeState.STOPPED:
                raise InteractiveRuntimeStoppedError(
                    "interactive runtime is stopped",
                    request_id=request_id,
                    workspace_session_id=workspace_session_id,
                )
            if (
                runtime_state is RuntimeState.BUSY
                or runtime_row["current_execution_id"] is not None
            ):
                raise InteractiveSessionBusyError(
                    "interactive runtime already has an active execution",
                    request_id=request_id,
                    workspace_session_id=workspace_session_id,
                    details={
                        "current_execution_id": runtime_row["current_execution_id"]
                    },
                )
            used = int(session_row["log_quota_used_bytes"])
            remaining = max(0, self._session_log_max_bytes - used)
            if remaining == 0:
                raise InteractiveInvalidRequestError(
                    "workspace session log quota is exhausted",
                    request_id=request_id,
                    workspace_session_id=workspace_session_id,
                    details={
                        "session_log_max_bytes": self._session_log_max_bytes,
                        "log_quota_used_bytes": used,
                        "remaining_log_quota_bytes": 0,
                        "requested_reservation_bytes": requested_reservation,
                    },
                )
            # The daemon reserves whatever remains instead of rejecting a
            # useful final execution merely because the per-execution ceiling
            # no longer fits in full.  This is computed inside the same write
            # transaction as the quota increment, so concurrent submissions
            # cannot overbook the session.
            reserve = min(requested_reservation, remaining)
            sequence = int(session_row["next_sequence"])
            provisional = ExecutionRecord(
                execution_id=execution_id,
                request_id=request_id,
                workspace_session_id=workspace_session_id,
                sequence=sequence,
                tool_kind=str(runtime_row["tool_kind"]),
                runtime_instance_id=str(runtime_row["runtime_instance_id"]),
                code=code,
                code_bytes=len(encoded),
                code_sha256=hashlib.sha256(encoded).hexdigest(),
                timeout_ms=timeout_ms,
                state=ExecutionState.QUEUED,
                submitted_at=timestamp,
                started_at=None,
                ended_at=None,
                exit_code=None,
                error=None,
                output_preview=None,
                preview_strategy=None,
                returned_bytes=None,
                output_bytes=None,
                output_lines=None,
                output_truncated=None,
                full_log_ref=full_log_ref,
                local_log_path=local_log_path,
                log_quota_reserved_bytes=reserve,
                log_quota_accounted_bytes=reserve,
                full_log_complete=None,
                full_log_written_bytes=None,
                full_log_dropped_bytes=None,
                full_log_incomplete_reason=None,
                runtime_preserved=None,
                log_retained=True,
            )
            worst_case = _worst_case_history_record(provisional, self._preview_max)
            worst_case_cursor = encode_history_cursor(workspace_session_id, sequence)
            worst_case_bytes = json_response_size(
                history_page_payload(
                    workspace_session_id,
                    (worst_case,),
                    has_more=True,
                    next_cursor=worst_case_cursor,
                )
            )
            if worst_case_bytes > self._history_bytes:
                raise InteractiveCodeTooLargeError(
                    "code cannot fit in one bounded history response",
                    request_id=request_id,
                    workspace_session_id=workspace_session_id,
                    details={
                        "actual_code_bytes": len(encoded),
                        "history_page_max_bytes": self._history_bytes,
                        "worst_case_history_bytes": worst_case_bytes,
                    },
                )
            idle_expires = timestamp_after(timestamp, self._runtime_idle_ttl)
            connection.execute(
                """
                INSERT INTO executions (
                    execution_id, request_id, workspace_session_id, sequence,
                    tool_kind, runtime_instance_id, code, code_bytes,
                    code_sha256, timeout_ms, state, submitted_at,
                    full_log_ref, local_log_path, log_quota_reserved_bytes,
                    log_quota_accounted_bytes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    execution_id,
                    request_id,
                    workspace_session_id,
                    sequence,
                    runtime_row["tool_kind"],
                    runtime_row["runtime_instance_id"],
                    code,
                    len(encoded),
                    provisional.code_sha256,
                    timeout_ms,
                    ExecutionState.QUEUED.value,
                    timestamp,
                    full_log_ref,
                    local_log_path,
                    reserve,
                    reserve,
                ),
            )
            connection.execute(
                """
                UPDATE workspace_sessions
                SET next_sequence = next_sequence + 1,
                    log_quota_used_bytes = log_quota_used_bytes + ?,
                    last_active_at = ?, idle_expires_at = ?
                WHERE workspace_session_id = ?
                """,
                (reserve, timestamp, idle_expires, workspace_session_id),
            )
            connection.execute(
                """
                UPDATE runtimes
                SET current_execution_id = ?, last_active_at = ?, idle_expires_at = ?
                WHERE workspace_session_id = ?
                """,
                (execution_id, timestamp, idle_expires, workspace_session_id),
            )
            self._append_event(
                connection,
                workspace_session_id,
                "EXECUTION_SUBMITTED",
                timestamp,
                runtime_instance_id=runtime_row["runtime_instance_id"],
                execution_id=execution_id,
            )
            row = connection.execute(
                "SELECT * FROM executions WHERE execution_id = ?",
                (execution_id,),
            ).fetchone()
        return ExecutionClaim(True, execution_from_row(row))

    def mark_execution_running(
        self,
        execution_id: str,
        *,
        now: str | None = None,
    ) -> ExecutionRecord:
        timestamp = now or utc_now()
        with self._transaction() as connection:
            row = self._require_execution(connection, execution_id)
            if row["state"] == ExecutionState.RUNNING.value:
                return execution_from_row(row)
            if row["state"] != ExecutionState.QUEUED.value:
                raise InteractiveInternalError(
                    f"execution {execution_id} cannot transition to RUNNING"
                )
            session_id = str(row["workspace_session_id"])
            runtime = self._require_runtime(connection, session_id)
            if runtime["current_execution_id"] != execution_id:
                raise InteractiveInternalError(
                    f"execution {execution_id} no longer owns its runtime"
                )
            if runtime["state"] != RuntimeState.READY.value:
                raise InteractiveInternalError(
                    f"runtime is not READY for execution {execution_id}"
                )
            connection.execute(
                "UPDATE executions SET state = ?, started_at = ? WHERE execution_id = ?",
                (ExecutionState.RUNNING.value, timestamp, execution_id),
            )
            connection.execute(
                """
                UPDATE runtimes
                SET state = ?, idle_expires_at = NULL
                WHERE workspace_session_id = ?
                """,
                (RuntimeState.BUSY.value, session_id),
            )
            connection.execute(
                """
                UPDATE workspace_sessions
                SET idle_expires_at = NULL
                WHERE workspace_session_id = ?
                """,
                (session_id,),
            )
            self._append_event(
                connection,
                session_id,
                "EXECUTION_STARTED",
                timestamp,
                transition={
                    "runtime": {
                        "from": RuntimeState.READY.value,
                        "to": RuntimeState.BUSY.value,
                    }
                },
                runtime_instance_id=runtime["runtime_instance_id"],
                execution_id=execution_id,
            )
            result = self._require_execution(connection, execution_id)
        return execution_from_row(result)

    def complete_execution(
        self,
        execution_id: str,
        state: ExecutionState | str,
        *,
        exit_code: int | None = None,
        error: dict[str, Any] | None = None,
        output_preview: str | None = None,
        preview_strategy: str | None = "diagnostics_head_tail",
        returned_bytes: int | None = None,
        output_bytes: int | None = None,
        output_lines: int | None = None,
        output_truncated: bool | None = None,
        full_log_complete: bool | None = None,
        full_log_written_bytes: int | None = None,
        full_log_dropped_bytes: int | None = None,
        full_log_incomplete_reason: str | None = None,
        runtime_preserved: bool,
        runtime_lost_reason: str | None = None,
        account_full_reservation: bool = False,
        now: str | None = None,
    ) -> ExecutionRecord:
        terminal_state = ExecutionState(state)
        if not terminal_state.terminal:
            raise InteractiveInternalError("execution completion requires a terminal state")
        timestamp = now or utc_now()
        normalized_error = bounded_error(error, self._preview_max)
        preview = (
            None
            if output_preview is None
            else bounded_text(output_preview, self._preview_max)
        )
        incomplete_reason = (
            None
            if full_log_incomplete_reason is None
            else bounded_text(full_log_incomplete_reason, self._preview_max)
        )
        with self._transaction() as connection:
            row = self._require_execution(connection, execution_id)
            current_state = ExecutionState(row["state"])
            if current_state.terminal:
                return execution_from_row(row)
            session_id = str(row["workspace_session_id"])
            runtime = self._require_runtime(connection, session_id)
            reserved = int(row["log_quota_reserved_bytes"])
            old_accounted = int(row["log_quota_accounted_bytes"])
            accounted = reserved if account_full_reservation else (
                reserved
                if full_log_written_bytes is None
                else max(0, min(int(full_log_written_bytes), reserved))
            )
            connection.execute(
                """
                UPDATE executions SET
                    state = ?, ended_at = ?, exit_code = ?, error_json = ?,
                    output_preview = ?, preview_strategy = ?, returned_bytes = ?,
                    output_bytes = ?, output_lines = ?, output_truncated = ?,
                    full_log_complete = ?, full_log_written_bytes = ?,
                    full_log_dropped_bytes = ?, full_log_incomplete_reason = ?,
                    runtime_preserved = ?, log_quota_accounted_bytes = ?
                WHERE execution_id = ?
                """,
                (
                    terminal_state.value,
                    timestamp,
                    exit_code,
                    dump_json(normalized_error),
                    preview,
                    preview_strategy,
                    optional_nonnegative(returned_bytes, "returned_bytes"),
                    optional_nonnegative(output_bytes, "output_bytes"),
                    optional_nonnegative(output_lines, "output_lines"),
                    to_db_bool(output_truncated),
                    to_db_bool(full_log_complete),
                    optional_nonnegative(full_log_written_bytes, "full_log_written_bytes"),
                    optional_nonnegative(full_log_dropped_bytes, "full_log_dropped_bytes"),
                    incomplete_reason,
                    int(bool(runtime_preserved)),
                    accounted,
                    execution_id,
                ),
            )
            idle_expires = timestamp_after(timestamp, self._runtime_idle_ttl)
            connection.execute(
                """
                UPDATE workspace_sessions
                SET log_quota_used_bytes = log_quota_used_bytes - ? + ?,
                    last_active_at = ?, idle_expires_at = ?
                WHERE workspace_session_id = ?
                """,
                (old_accounted, accounted, timestamp, idle_expires, session_id),
            )
            if runtime_preserved:
                connection.execute(
                    """
                    UPDATE runtimes
                    SET state = ?, current_execution_id = NULL,
                        last_active_at = ?, idle_expires_at = ?
                    WHERE workspace_session_id = ?
                    """,
                    (RuntimeState.READY.value, timestamp, idle_expires, session_id),
                )
            else:
                reason = runtime_lost_reason or terminal_state.value
                connection.execute(
                    """
                    UPDATE runtimes
                    SET state = ?, capacity_lease_id = NULL,
                        current_execution_id = NULL, lost_reason = ?, lost_at = ?,
                        stopped_at = ?, last_active_at = ?, idle_expires_at = NULL
                    WHERE workspace_session_id = ?
                    """,
                    (
                        RuntimeState.LOST.value,
                        reason,
                        timestamp,
                        timestamp,
                        timestamp,
                        session_id,
                    ),
                )
            event = {
                ExecutionState.SUCCEEDED: "EXECUTION_SUCCEEDED",
                ExecutionState.FAILED: "EXECUTION_FAILED",
                ExecutionState.TIMED_OUT: "EXECUTION_TIMED_OUT",
                ExecutionState.LOST: "EXECUTION_LOST",
            }[terminal_state]
            self._append_event(
                connection,
                session_id,
                event,
                timestamp,
                transition={
                    "runtime": {
                        "from": runtime["state"],
                        "to": (
                            RuntimeState.READY.value
                            if runtime_preserved
                            else RuntimeState.LOST.value
                        ),
                    }
                },
                runtime_instance_id=runtime["runtime_instance_id"],
                execution_id=execution_id,
                reason=normalized_error,
            )
            if not runtime_preserved:
                lost_reason = runtime_lost_reason or terminal_state.value
                self._append_event(
                    connection,
                    session_id,
                    "RUNTIME_LOST",
                    timestamp,
                    transition={
                        "runtime": {
                            "from": runtime["state"],
                            "to": RuntimeState.LOST.value,
                        }
                    },
                    runtime_instance_id=runtime["runtime_instance_id"],
                    execution_id=execution_id,
                    reason={"code": lost_reason, "message": lost_reason},
                )
            if not runtime_preserved and all(
                runtime[field] is None
                for field in (
                    "worker_process_id",
                    "process_id",
                    "capacity_lease_id",
                )
            ):
                # Some failure paths must terminate and drain the process tree
                # before they can trust the partial log summary. In that
                # ordering ownership is already cleared while the runtime is
                # still BUSY/READY, so close the release lifecycle here in the
                # same transaction as the LOST transition.
                self._append_runtime_released_once(
                    connection,
                    session_id,
                    timestamp,
                    runtime_instance_id=runtime["runtime_instance_id"],
                    reason=lost_reason,
                )
            result = self._require_execution(connection, execution_id)
        return execution_from_row(result)

    def get_execution(self, execution_id: str) -> ExecutionRecord:
        with self._connect() as connection:
            row = self._require_execution(connection, execution_id)
        return execution_from_row(row)

    def list_unsealed_lost_executions(
        self,
        workspace_session_id: str,
    ) -> tuple[ExecutionRecord, ...]:
        """Return LOST records whose process-owned log is not sealed yet.

        Lifecycle transitions intentionally mark an active execution LOST in a
        short transaction before process termination can be confirmed.  A
        ``NULL`` written byte count is the durable hand-off to the service: it
        must drain/stop the process tree and then recover the bounded log.
        """

        with self._connect() as connection:
            self._require_session(connection, workspace_session_id)
            rows = connection.execute(
                """
                SELECT * FROM executions
                WHERE workspace_session_id = ? AND state = ?
                  AND full_log_complete = 0
                  AND full_log_written_bytes IS NULL
                ORDER BY sequence ASC
                """,
                (workspace_session_id, ExecutionState.LOST.value),
            ).fetchall()
        return tuple(execution_from_row(row) for row in rows)

    def list_safe_unsealed_lost_sessions(self) -> tuple[str, ...]:
        """Return sessions whose LOST logs have no durable process owner.

        This is the restart/sweep discovery path for a transient partial-log
        read failure.  PID ownership is cleared only after the daemon has
        confirmed that the runtime process tree is gone, so scanning these
        rows cannot race a remaining log writer.
        """

        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT e.workspace_session_id
                FROM executions AS e
                JOIN runtimes AS r USING (workspace_session_id)
                WHERE e.state = ?
                  AND e.full_log_complete = 0
                  AND e.full_log_written_bytes IS NULL
                  AND r.state IN (?, ?)
                  AND r.worker_process_id IS NULL
                  AND r.process_id IS NULL
                ORDER BY e.workspace_session_id ASC
                """,
                (
                    ExecutionState.LOST.value,
                    RuntimeState.LOST.value,
                    RuntimeState.STOPPED.value,
                ),
            ).fetchall()
        return tuple(str(row[0]) for row in rows)

    def seal_lost_execution_log(
        self,
        execution_id: str,
        *,
        output_preview: str,
        returned_bytes: int,
        output_bytes: int,
        output_lines: int,
        output_truncated: bool,
        full_log_written_bytes: int,
        full_log_incomplete_reason: str,
    ) -> ExecutionRecord:
        """Idempotently seal diagnostics for a process-confirmed LOST record.

        This method never changes a non-LOST record or a LOST record which
        already has a trustworthy worker/service summary.  The full durable
        reservation remains accounted because a missing worker summary cannot
        prove that unwritten quota is safe to release.
        """

        preview = bounded_text(output_preview, self._preview_max)
        reason = bounded_text(full_log_incomplete_reason, self._preview_max)
        returned = optional_nonnegative(returned_bytes, "returned_bytes")
        total_bytes = optional_nonnegative(output_bytes, "output_bytes")
        total_lines = optional_nonnegative(output_lines, "output_lines")
        written = optional_nonnegative(
            full_log_written_bytes,
            "full_log_written_bytes",
        )
        with self._transaction() as connection:
            row = self._require_execution(connection, execution_id)
            if not (
                row["state"] == ExecutionState.LOST.value
                and row["full_log_complete"] == 0
                and row["full_log_written_bytes"] is None
            ):
                return execution_from_row(row)

            reserved = int(row["log_quota_reserved_bytes"])
            old_accounted = int(row["log_quota_accounted_bytes"])
            connection.execute(
                """
                UPDATE executions
                SET output_preview = ?, preview_strategy = ?, returned_bytes = ?,
                    output_bytes = ?, output_lines = ?, output_truncated = ?,
                    full_log_complete = 0, full_log_written_bytes = ?,
                    full_log_dropped_bytes = NULL,
                    full_log_incomplete_reason = ?, runtime_preserved = 0,
                    log_quota_accounted_bytes = ?
                WHERE execution_id = ?
                """,
                (
                    preview,
                    "head_tail",
                    returned,
                    total_bytes,
                    total_lines,
                    int(bool(output_truncated)),
                    min(written, reserved),
                    reason,
                    reserved,
                    execution_id,
                ),
            )
            connection.execute(
                """
                UPDATE workspace_sessions
                SET log_quota_used_bytes = log_quota_used_bytes - ? + ?
                WHERE workspace_session_id = ?
                """,
                (old_accounted, reserved, row["workspace_session_id"]),
            )
            result = self._require_execution(connection, execution_id)
        return execution_from_row(result)

    def get_execution_by_request(
        self,
        workspace_session_id: str,
        request_id: str,
    ) -> ExecutionRecord | None:
        with self._connect() as connection:
            self._require_session(connection, workspace_session_id)
            row = connection.execute(
                """
                SELECT * FROM executions
                WHERE workspace_session_id = ? AND request_id = ?
                """,
                (workspace_session_id, request_id),
            ).fetchone()
        return None if row is None else execution_from_row(row)

    def history_page(
        self,
        workspace_session_id: str,
        *,
        limit: int | None = None,
        cursor: str | None = None,
        max_page_bytes: int | None = None,
    ) -> HistoryPage:
        page_limit = self._history_default if limit is None else limit
        if (
            isinstance(page_limit, bool)
            or not isinstance(page_limit, int)
            or page_limit < 1
            or page_limit > self._history_max
        ):
            raise InteractiveInvalidRequestError(
                f"history limit must be between 1 and {self._history_max}",
                workspace_session_id=workspace_session_id,
            )
        budget = self._history_bytes if max_page_bytes is None else max_page_bytes
        if isinstance(budget, bool) or not isinstance(budget, int) or budget <= 0:
            raise InteractiveInvalidRequestError("history byte budget must be positive")
        after_sequence = (
            0 if cursor is None else decode_history_cursor(cursor, workspace_session_id)
        )
        with self._connect() as connection:
            self._require_session(connection, workspace_session_id)
            rows = connection.execute(
                """
                SELECT executions.*,
                       EXISTS (
                           SELECT 1
                           FROM executions AS later
                           WHERE later.workspace_session_id =
                                 executions.workspace_session_id
                             AND later.sequence > executions.sequence
                       ) AS history_has_more
                FROM executions
                WHERE executions.workspace_session_id = ?
                  AND executions.sequence > ?
                ORDER BY executions.sequence ASC
                LIMIT ?
                """,
                (workspace_session_id, after_sequence, page_limit),
            )
            records: list[ExecutionRecord] = []
            response_bytes = json_response_size(
                history_page_payload(
                    workspace_session_id,
                    (),
                    has_more=False,
                    next_cursor=None,
                )
            )
            has_more = False
            while len(records) < page_limit:
                # Do not materialize ``limit + 1`` potentially MiB-sized code
                # values.  SQLite keeps the result cursor and Python retains
                # only the accepted page plus this one candidate row.
                row = rows.fetchone()
                if row is None:
                    break
                record = execution_from_row(row)
                candidate = (*records, record)
                candidate_has_more = bool(row["history_has_more"])
                candidate_cursor = (
                    encode_history_cursor(workspace_session_id, record.sequence)
                    if candidate_has_more
                    else None
                )
                candidate_bytes = json_response_size(
                    history_page_payload(
                        workspace_session_id,
                        candidate,
                        has_more=candidate_has_more,
                        next_cursor=candidate_cursor,
                    )
                )
                if candidate_bytes > budget:
                    if not records:
                        raise InteractiveInternalError(
                            "stored execution cannot fit in the configured "
                            "history page budget",
                            workspace_session_id=workspace_session_id,
                            details={
                                "execution_id": record.execution_id,
                                "history_page_max_bytes": budget,
                                "required_bytes": candidate_bytes,
                            },
                        )
                    # The rejected candidate itself proves that another page
                    # exists, even when it was the final row in the registry.
                    has_more = True
                    break
                records.append(record)
                response_bytes = candidate_bytes
                has_more = candidate_has_more
        next_cursor = (
            encode_history_cursor(workspace_session_id, records[-1].sequence)
            if has_more and records
            else None
        )
        # If the next record exceeded the budget, the last candidate above was
        # measured with ``has_more=True`` already.  Recompute defensively so the
        # returned byte count always describes the exact public envelope.
        response_bytes = json_response_size(
            history_page_payload(
                workspace_session_id,
                records,
                has_more=has_more,
                next_cursor=next_cursor,
            )
        )
        if response_bytes > budget:  # pragma: no cover - admission/loop guard
            raise InteractiveInternalError(
                "history response exceeded its configured byte budget",
                workspace_session_id=workspace_session_id,
                details={
                    "history_page_max_bytes": budget,
                    "required_bytes": response_bytes,
                },
            )
        return HistoryPage(
            workspace_session_id=workspace_session_id,
            records=tuple(records),
            has_more=has_more,
            next_cursor=next_cursor,
            estimated_bytes=response_bytes,
        )

    def release_execution_log_quota(
        self,
        execution_id: str,
        *,
        now: str | None = None,
    ) -> ExecutionRecord:
        timestamp = now or utc_now()
        with self._transaction() as connection:
            execution = self._require_execution(connection, execution_id)
            if not bool(execution["log_retained"]):
                return execution_from_row(execution)
            accounted = int(execution["log_quota_accounted_bytes"])
            session_id = str(execution["workspace_session_id"])
            connection.execute(
                """
                UPDATE executions
                SET log_retained = 0, log_quota_accounted_bytes = 0
                WHERE execution_id = ?
                """,
                (execution_id,),
            )
            connection.execute(
                """
                UPDATE workspace_sessions
                SET log_quota_used_bytes = MAX(0, log_quota_used_bytes - ?)
                WHERE workspace_session_id = ?
                """,
                (accounted, session_id),
            )
            self._append_event(
                connection,
                session_id,
                "LOG_EXPIRED",
                timestamp,
                runtime_instance_id=execution["runtime_instance_id"],
                execution_id=execution_id,
            )
            result = self._require_execution(connection, execution_id)
        return execution_from_row(result)
