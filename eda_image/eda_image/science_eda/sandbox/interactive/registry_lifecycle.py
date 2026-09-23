"""Runtime/session lifecycle, retention, events, and restart reconciliation."""

from __future__ import annotations

from science_eda.exceptions import InteractiveSessionBusyError
from science_eda.sandbox.interactive.models import (
    CreateRequestState,
    ExecutionState,
    GarbageCollectionResult,
    OrphanRuntimeRecord,
    ReconciliationResult,
    RuntimeRecord,
    RuntimeState,
    SessionEventRecord,
    SessionState,
    WorkspaceSessionRecord,
    timestamp_after,
    utc_now,
)
from science_eda.sandbox.interactive.registry_codec import (
    bounded_text,
    dump_json,
    event_from_row,
    execution_from_row,
    runtime_from_row,
    session_from_row,
)
from science_eda.sandbox.interactive.registry_constants import (
    ACTIVE_EXECUTION_STATES,
)


class RegistryLifecycleMixin:
    """Persist runtime loss/stopping, session closure, and retention work."""

    def clear_runtime_process_ownership(self, workspace_session_id: str) -> None:
        """Forget PIDs only after the service has confirmed both processes exited."""

        timestamp = utc_now()
        with self._transaction() as connection:
            runtime = self._require_runtime(connection, workspace_session_id)
            had_process_ownership = any(
                runtime[field] is not None
                for field in (
                    "worker_process_id",
                    "worker_process_identity",
                    "process_id",
                    "process_identity",
                    "capacity_lease_id",
                )
            )
            connection.execute(
                """
                UPDATE runtimes
                SET worker_process_id = NULL, worker_process_identity = NULL,
                    process_id = NULL, process_identity = NULL,
                    capacity_lease_id = NULL
                WHERE workspace_session_id = ?
                """,
                (workspace_session_id,),
            )
            if had_process_ownership and runtime["state"] in {
                RuntimeState.LOST.value,
                RuntimeState.STOPPED.value,
            }:
                self._append_runtime_released_once(
                    connection,
                    workspace_session_id,
                    timestamp,
                    runtime_instance_id=runtime["runtime_instance_id"],
                    reason="RUNTIME_TERMINATED",
                )

    def mark_runtime_lost(
        self,
        workspace_session_id: str,
        reason: str,
        *,
        now: str | None = None,
    ) -> RuntimeRecord:
        timestamp = now or utc_now()
        with self._transaction() as connection:
            runtime = self._require_runtime(connection, workspace_session_id)
            if runtime["state"] in {
                RuntimeState.LOST.value,
                RuntimeState.STOPPED.value,
            }:
                return runtime_from_row(runtime)
            current_execution_id = runtime["current_execution_id"]
            if current_execution_id is not None:
                execution = self._require_execution(connection, current_execution_id)
                if execution["state"] in ACTIVE_EXECUTION_STATES:
                    connection.execute(
                        """
                        UPDATE executions
                        SET state = ?, ended_at = ?, runtime_preserved = 0,
                            error_json = ?, full_log_complete = 0,
                            full_log_written_bytes = NULL,
                            full_log_dropped_bytes = NULL,
                            full_log_incomplete_reason = ?
                        WHERE execution_id = ?
                        """,
                        (
                            ExecutionState.LOST.value,
                            timestamp,
                            dump_json(
                                {
                                    "code": "PROCESS_LOST",
                                    "message": bounded_text(reason, self._preview_max),
                                }
                            ),
                            "PROCESS_LOST",
                            current_execution_id,
                        ),
                    )
                    self._append_event(
                        connection,
                        workspace_session_id,
                        "EXECUTION_LOST",
                        timestamp,
                        runtime_instance_id=runtime["runtime_instance_id"],
                        execution_id=current_execution_id,
                        reason={"code": "PROCESS_LOST", "message": reason},
                    )
            connection.execute(
                """
                UPDATE runtimes
                SET state = ?, capacity_lease_id = NULL,
                    current_execution_id = NULL, lost_reason = ?, lost_at = ?,
                    stopped_at = ?, idle_expires_at = NULL
                WHERE workspace_session_id = ?
                """,
                (
                    RuntimeState.LOST.value,
                    bounded_text(reason, self._preview_max),
                    timestamp,
                    timestamp,
                    workspace_session_id,
                ),
            )
            self._append_event(
                connection,
                workspace_session_id,
                "RUNTIME_LOST",
                timestamp,
                transition={
                    "runtime": {
                        "from": runtime["state"],
                        "to": RuntimeState.LOST.value,
                    }
                },
                runtime_instance_id=runtime["runtime_instance_id"],
                execution_id=current_execution_id,
                reason={"code": "PROCESS_LOST", "message": reason},
            )
            result = self._require_runtime(connection, workspace_session_id)
        return runtime_from_row(result)

    def mark_runtime_stopped(
        self,
        workspace_session_id: str,
        *,
        reason: str = "WARM_TTL_EXPIRED",
        now: str | None = None,
    ) -> RuntimeRecord:
        timestamp = now or utc_now()
        with self._transaction() as connection:
            runtime = self._require_runtime(connection, workspace_session_id)
            if runtime["state"] == RuntimeState.STOPPED.value:
                return runtime_from_row(runtime)
            if runtime["state"] == RuntimeState.BUSY.value or runtime["current_execution_id"]:
                raise InteractiveSessionBusyError(
                    "busy runtime cannot be stopped by idle retention",
                    workspace_session_id=workspace_session_id,
                    details={"current_execution_id": runtime["current_execution_id"]},
                )
            if runtime["state"] == RuntimeState.LOST.value:
                return runtime_from_row(runtime)
            connection.execute(
                """
                UPDATE runtimes
                SET state = ?, worker_process_id = NULL,
                    worker_process_identity = NULL, process_id = NULL,
                    process_identity = NULL,
                    capacity_lease_id = NULL,
                    current_execution_id = NULL, stopped_at = ?, idle_expires_at = NULL
                WHERE workspace_session_id = ?
                """,
                (RuntimeState.STOPPED.value, timestamp, workspace_session_id),
            )
            self._append_event(
                connection,
                workspace_session_id,
                "WARM_TTL_EXPIRED",
                timestamp,
                transition={
                    "runtime": {
                        "from": runtime["state"],
                        "to": RuntimeState.STOPPED.value,
                    }
                },
                runtime_instance_id=runtime["runtime_instance_id"],
                reason={"code": reason, "message": reason},
            )
            self._append_runtime_released_once(
                connection,
                workspace_session_id,
                timestamp,
                runtime_instance_id=runtime["runtime_instance_id"],
                reason=reason,
            )
            result = self._require_runtime(connection, workspace_session_id)
        return runtime_from_row(result)

    def close_workspace_session(
        self,
        workspace_session_id: str,
        *,
        runtime_terminated: bool,
        reason: str = "USER_DESTROY",
        now: str | None = None,
    ) -> WorkspaceSessionRecord:
        timestamp = now or utc_now()
        audit_expires = timestamp_after(timestamp, self._audit_retention)
        with self._transaction() as connection:
            session = self._require_session(connection, workspace_session_id)
            if session["state"] == SessionState.CLOSED.value:
                return session_from_row(session)
            runtime = self._require_runtime(connection, workspace_session_id)
            self._append_event(
                connection,
                workspace_session_id,
                "SESSION_DESTROYING",
                timestamp,
                transition={
                    "session": {
                        "from": session["state"],
                        "to": SessionState.CLOSING.value,
                    }
                },
                runtime_instance_id=runtime["runtime_instance_id"],
                execution_id=runtime["current_execution_id"],
                reason={"code": reason, "message": reason},
            )
            connection.execute(
                "UPDATE workspace_sessions SET state = ? WHERE workspace_session_id = ?",
                (SessionState.CLOSING.value, workspace_session_id),
            )
            current_execution_id = runtime["current_execution_id"]
            if current_execution_id is not None:
                execution = self._require_execution(connection, current_execution_id)
                if execution["state"] in ACTIVE_EXECUTION_STATES:
                    connection.execute(
                        """
                        UPDATE executions
                        SET state = ?, ended_at = ?, runtime_preserved = 0,
                            error_json = ?, full_log_complete = 0,
                            full_log_written_bytes = NULL,
                            full_log_dropped_bytes = NULL,
                            full_log_incomplete_reason = ?
                        WHERE execution_id = ?
                        """,
                        (
                            ExecutionState.LOST.value,
                            timestamp,
                            dump_json({"code": reason, "message": reason}),
                            bounded_text(reason, self._preview_max),
                            current_execution_id,
                        ),
                    )
                    self._append_event(
                        connection,
                        workspace_session_id,
                        "EXECUTION_LOST",
                        timestamp,
                        runtime_instance_id=runtime["runtime_instance_id"],
                        execution_id=current_execution_id,
                        reason={"code": reason, "message": reason},
                    )
            close_result = {
                "workspace_session_id": workspace_session_id,
                "state": SessionState.CLOSED.value,
                "runtime_terminated": bool(runtime_terminated),
                "closed_at": timestamp,
            }
            connection.execute(
                """
                UPDATE runtimes
                SET state = ?,
                    worker_process_id = CASE WHEN ? THEN NULL ELSE worker_process_id END,
                    worker_process_identity = CASE WHEN ? THEN NULL ELSE worker_process_identity END,
                    process_id = CASE WHEN ? THEN NULL ELSE process_id END,
                    process_identity = CASE WHEN ? THEN NULL ELSE process_identity END,
                    capacity_lease_id = NULL,
                    current_execution_id = NULL, stopped_at = ?, idle_expires_at = NULL
                WHERE workspace_session_id = ?
                """,
                (
                    RuntimeState.STOPPED.value,
                    int(bool(runtime_terminated)),
                    int(bool(runtime_terminated)),
                    int(bool(runtime_terminated)),
                    int(bool(runtime_terminated)),
                    timestamp,
                    workspace_session_id,
                ),
            )
            if runtime_terminated:
                self._append_runtime_released_once(
                    connection,
                    workspace_session_id,
                    timestamp,
                    runtime_instance_id=runtime["runtime_instance_id"],
                    reason=reason,
                )
            connection.execute(
                """
                UPDATE workspace_sessions
                SET state = ?, closed_at = ?, audit_expires_at = ?,
                    close_result_json = ?, idle_expires_at = NULL
                WHERE workspace_session_id = ?
                """,
                (
                    SessionState.CLOSED.value,
                    timestamp,
                    audit_expires,
                    dump_json(close_result),
                    workspace_session_id,
                ),
            )
            connection.execute(
                """
                UPDATE create_requests
                SET retain_until = MAX(retain_until, ?)
                WHERE workspace_session_id = ?
                """,
                (audit_expires, workspace_session_id),
            )
            self._append_event(
                connection,
                workspace_session_id,
                "SESSION_DESTROYED",
                timestamp,
                transition={
                    "session": {
                        "from": SessionState.CLOSING.value,
                        "to": SessionState.CLOSED.value,
                    },
                    "runtime": {
                        "from": runtime["state"],
                        "to": RuntimeState.STOPPED.value,
                    },
                },
                runtime_instance_id=runtime["runtime_instance_id"],
                execution_id=current_execution_id,
                reason={"code": reason, "message": reason},
            )
            result = self._require_session(connection, workspace_session_id)
        return session_from_row(result)

    def _append_runtime_released_once(
        self,
        connection,
        workspace_session_id: str,
        occurred_at: str,
        *,
        runtime_instance_id: str,
        reason: str,
    ) -> None:
        existing = connection.execute(
            """
            SELECT 1 FROM session_events
            WHERE workspace_session_id = ? AND event_type = ?
              AND runtime_instance_id = ?
            LIMIT 1
            """,
            (
                workspace_session_id,
                "RUNTIME_RELEASED",
                runtime_instance_id,
            ),
        ).fetchone()
        if existing is not None:
            return
        self._append_event(
            connection,
            workspace_session_id,
            "RUNTIME_RELEASED",
            occurred_at,
            runtime_instance_id=runtime_instance_id,
            reason={"code": reason, "message": reason},
        )

    def touch_runtime(
        self,
        workspace_session_id: str,
        *,
        now: str | None = None,
    ) -> RuntimeRecord:
        timestamp = now or utc_now()
        idle_expires = timestamp_after(timestamp, self._runtime_idle_ttl)
        with self._transaction() as connection:
            runtime = self._require_runtime(connection, workspace_session_id)
            if runtime["state"] not in {
                RuntimeState.READY.value,
                RuntimeState.BUSY.value,
            }:
                return runtime_from_row(runtime)
            connection.execute(
                """
                UPDATE runtimes
                SET last_active_at = ?, idle_expires_at = ?
                WHERE workspace_session_id = ?
                """,
                (timestamp, idle_expires, workspace_session_id),
            )
            connection.execute(
                """
                UPDATE workspace_sessions
                SET last_active_at = ?, idle_expires_at = ?
                WHERE workspace_session_id = ?
                """,
                (timestamp, idle_expires, workspace_session_id),
            )
            result = self._require_runtime(connection, workspace_session_id)
        return runtime_from_row(result)

    def list_idle_runtime_candidates(
        self,
        *,
        now: str | None = None,
    ) -> tuple[RuntimeRecord, ...]:
        timestamp = now or utc_now()
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM runtimes
                WHERE state = ? AND current_execution_id IS NULL
                  AND idle_expires_at IS NOT NULL AND idle_expires_at <= ?
                ORDER BY idle_expires_at
                """,
                (RuntimeState.READY.value, timestamp),
            ).fetchall()
        return tuple(runtime_from_row(row) for row in rows)

    def list_sessions_due_for_retention(
        self,
        *,
        now: str | None = None,
    ) -> tuple[str, ...]:
        if self._session_retention_ttl == 0:
            return ()
        timestamp = now or utc_now()
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT s.workspace_session_id, COALESCE(r.stopped_at, r.lost_at)
                FROM workspace_sessions AS s
                JOIN runtimes AS r USING (workspace_session_id)
                WHERE s.state = ? AND r.state IN (?, ?)
                  AND COALESCE(r.stopped_at, r.lost_at) IS NOT NULL
                """,
                (
                    SessionState.ACTIVE.value,
                    RuntimeState.STOPPED.value,
                    RuntimeState.LOST.value,
                ),
            ).fetchall()
        return tuple(
            str(row[0])
            for row in rows
            if timestamp_after(str(row[1]), self._session_retention_ttl) <= timestamp
        )

    def list_session_events(
        self,
        workspace_session_id: str,
    ) -> tuple[SessionEventRecord, ...]:
        with self._connect() as connection:
            self._require_session(connection, workspace_session_id)
            rows = connection.execute(
                """
                SELECT * FROM session_events
                WHERE workspace_session_id = ?
                ORDER BY event_sequence ASC
                """,
                (workspace_session_id,),
            ).fetchall()
        return tuple(event_from_row(row) for row in rows)

    def reconcile_after_restart(
        self,
        *,
        now: str | None = None,
    ) -> ReconciliationResult:
        timestamp = now or utc_now()
        with self._transaction() as connection:
            startup_rows = connection.execute(
                "SELECT * FROM runtime_startups ORDER BY created_at, request_id"
            ).fetchall()
            pending_rows = connection.execute(
                "SELECT request_id FROM create_requests WHERE state = ?",
                (CreateRequestState.PENDING.value,),
            ).fetchall()
            connection.execute(
                """
                UPDATE create_requests
                SET state = ?, error_code = ?, error_message = ?, completed_at = ?,
                    retain_until = MAX(retain_until, ?)
                WHERE state = ?
                """,
                (
                    CreateRequestState.FAILED.value,
                    "RUNTIME_START_FAILED",
                    "interactive daemon restarted before create completed",
                    timestamp,
                    timestamp_after(timestamp, self._create_retention),
                    CreateRequestState.PENDING.value,
                ),
            )
            execution_rows = connection.execute(
                """
                SELECT * FROM executions WHERE state IN (?, ?)
                """,
                ACTIVE_EXECUTION_STATES,
            ).fetchall()
            for execution in execution_rows:
                connection.execute(
                    """
                    UPDATE executions
                    SET state = ?, ended_at = ?, runtime_preserved = 0,
                        error_json = ?, full_log_complete = 0,
                        full_log_written_bytes = NULL,
                        full_log_dropped_bytes = NULL,
                        full_log_incomplete_reason = ?
                    WHERE execution_id = ?
                    """,
                    (
                        ExecutionState.LOST.value,
                        timestamp,
                        dump_json(
                            {
                                "code": "DAEMON_RESTART",
                                "message": "daemon restarted during execution",
                            }
                        ),
                        "DAEMON_RESTART",
                        execution["execution_id"],
                    ),
                )
                self._append_event(
                    connection,
                    execution["workspace_session_id"],
                    "EXECUTION_LOST",
                    timestamp,
                    runtime_instance_id=execution["runtime_instance_id"],
                    execution_id=execution["execution_id"],
                    reason={
                        "code": "DAEMON_RESTART",
                        "message": "daemon restarted during execution",
                    },
                )
            runtime_rows = connection.execute(
                "SELECT * FROM runtimes WHERE state IN (?, ?)",
                (RuntimeState.READY.value, RuntimeState.BUSY.value),
            ).fetchall()
            orphan_runtime_rows = connection.execute(
                """
                SELECT * FROM runtimes
                WHERE worker_process_id IS NOT NULL OR process_id IS NOT NULL
                """
            ).fetchall()
            for runtime in runtime_rows:
                connection.execute(
                    """
                    UPDATE runtimes
                    SET state = ?, capacity_lease_id = NULL,
                        current_execution_id = NULL, lost_reason = ?, lost_at = ?,
                        stopped_at = ?, idle_expires_at = NULL
                    WHERE workspace_session_id = ?
                    """,
                    (
                        RuntimeState.LOST.value,
                        "DAEMON_RESTART",
                        timestamp,
                        timestamp,
                        runtime["workspace_session_id"],
                    ),
                )
                self._append_event(
                    connection,
                    runtime["workspace_session_id"],
                    "RUNTIME_LOST",
                    timestamp,
                    transition={
                        "runtime": {
                            "from": runtime["state"],
                            "to": RuntimeState.LOST.value,
                        }
                    },
                    runtime_instance_id=runtime["runtime_instance_id"],
                    execution_id=runtime["current_execution_id"],
                    reason={
                        "code": "DAEMON_RESTART",
                        "message": "live runtime is not recoverable after daemon restart",
                    },
                )
            result = ReconciliationResult(
                pending_creates_failed=len(pending_rows),
                executions_lost=len(execution_rows),
                runtimes_lost=len(runtime_rows),
                scratch_relative_paths=tuple(
                    dict.fromkeys(
                        [str(row["scratch_relative_path"]) for row in startup_rows]
                        + [
                            str(row["scratch_relative_path"])
                            for row in orphan_runtime_rows
                        ]
                    )
                ),
                orphan_runtimes=tuple(
                    [
                        OrphanRuntimeRecord(
                            request_id=str(row["request_id"]),
                            workspace_session_id=str(row["workspace_session_id"]),
                            tool_kind=str(row["tool_kind"]),
                            scratch_relative_path=str(row["scratch_relative_path"]),
                            worker_process_id=(
                                None
                                if row["worker_process_id"] is None
                                else int(row["worker_process_id"])
                            ),
                            worker_process_identity=row[
                                "worker_process_identity"
                            ],
                            process_id=(
                                None
                                if row["process_id"] is None
                                else int(row["process_id"])
                            ),
                            process_identity=row["process_identity"],
                            ownership_unknown=(
                                bool(row["spawn_attempted"])
                                and row["process_id"] is None
                            ),
                        )
                        for row in startup_rows
                    ]
                    + [
                        OrphanRuntimeRecord(
                            request_id=None,
                            workspace_session_id=str(row["workspace_session_id"]),
                            tool_kind=str(row["tool_kind"]),
                            scratch_relative_path=str(row["scratch_relative_path"]),
                            worker_process_id=(
                                None
                                if row["worker_process_id"] is None
                                else int(row["worker_process_id"])
                            ),
                            worker_process_identity=row[
                                "worker_process_identity"
                            ],
                            process_id=(
                                None
                                if row["process_id"] is None
                                else int(row["process_id"])
                            ),
                            process_identity=row["process_identity"],
                            ownership_unknown=False,
                        )
                        for row in orphan_runtime_rows
                    ]
                ),
            )
        with self._terminal_condition:
            self._terminal_condition.notify_all()
        return result

    def gc_retention(
        self,
        *,
        now: str | None = None,
    ) -> GarbageCollectionResult:
        timestamp = now or utc_now()
        with self._transaction() as connection:
            session_rows = connection.execute(
                """
                SELECT s.workspace_session_id, r.scratch_relative_path
                FROM workspace_sessions AS s
                JOIN runtimes AS r USING (workspace_session_id)
                WHERE s.state = ? AND s.audit_expires_at IS NOT NULL
                  AND s.audit_expires_at <= ?
                  AND r.worker_process_id IS NULL AND r.process_id IS NULL
                """,
                (SessionState.CLOSED.value, timestamp),
            ).fetchall()
            session_ids = tuple(str(row[0]) for row in session_rows)
            log_paths: list[str] = []
            if session_ids:
                placeholders = ",".join("?" for _ in session_ids)
                log_paths = [
                    str(row[0])
                    for row in connection.execute(
                        f"""
                        SELECT local_log_path FROM executions
                        WHERE workspace_session_id IN ({placeholders})
                          AND log_retained = 1
                        """,
                        session_ids,
                    ).fetchall()
                ]
            create_rows = connection.execute(
                """
                SELECT request_id FROM create_requests AS c
                WHERE c.state != ? AND c.retain_until <= ?
                  AND NOT EXISTS (
                    SELECT 1 FROM workspace_sessions AS s
                    WHERE s.workspace_session_id = c.workspace_session_id
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM runtime_startups AS rs
                    WHERE rs.request_id = c.request_id
                  )
                """,
                (CreateRequestState.PENDING.value, timestamp),
            ).fetchall()
            create_ids = tuple(str(row[0]) for row in create_rows)
            if create_ids:
                placeholders = ",".join("?" for _ in create_ids)
                connection.execute(
                    f"DELETE FROM create_requests WHERE request_id IN ({placeholders})",
                    create_ids,
                )
            return GarbageCollectionResult(
                create_request_ids=create_ids,
                workspace_session_ids=session_ids,
                scratch_relative_paths=tuple(str(row[1]) for row in session_rows),
                local_log_paths=tuple(log_paths),
            )

    def finalize_session_gc(
        self,
        workspace_session_id: str,
        *,
        now: str | None = None,
    ) -> bool:
        """Delete one retained session after its daemon-owned files are gone.

        Keeping the registry row until filesystem cleanup succeeds makes the
        sweep crash-safe: a later daemon can still discover and retry a failed
        scratch or log removal instead of losing the only durable pointer.
        """

        timestamp = now or utc_now()
        with self._transaction() as connection:
            deleted = connection.execute(
                """
                DELETE FROM workspace_sessions
                WHERE workspace_session_id = ?
                  AND state = ?
                  AND audit_expires_at IS NOT NULL
                  AND audit_expires_at <= ?
                  AND EXISTS (
                    SELECT 1 FROM runtimes AS r
                    WHERE r.workspace_session_id = workspace_sessions.workspace_session_id
                      AND r.worker_process_id IS NULL AND r.process_id IS NULL
                  )
                """,
                (
                    workspace_session_id,
                    SessionState.CLOSED.value,
                    timestamp,
                ),
            ).rowcount
            return bool(deleted)
