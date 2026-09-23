"""Durable ownership for create tasks which have not published a Session."""

from __future__ import annotations

from science_eda.exceptions import InteractiveInvalidRequestError
from science_eda.sandbox.interactive.models import RuntimeStartupRecord, utc_now
from science_eda.sandbox.interactive.registry_codec import runtime_startup_from_row


class RegistryStartupMixin:
    """Persist scratch and PIDs before a create task can become orphaned."""

    def register_runtime_startup(
        self,
        request_id: str,
        workspace_session_id: str,
        scratch_relative_path: str,
        *,
        now: str | None = None,
    ) -> RuntimeStartupRecord:
        timestamp = now or utc_now()
        with self._transaction() as connection:
            create = self._require_create(connection, request_id)
            if create["state"] != "PENDING":
                raise InteractiveInvalidRequestError(
                    "runtime ownership requires a pending create request",
                    request_id=request_id,
                )
            existing = connection.execute(
                "SELECT * FROM runtime_startups WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["workspace_session_id"] != workspace_session_id
                    or existing["scratch_relative_path"] != scratch_relative_path
                ):
                    raise InteractiveInvalidRequestError(
                        "create request already owns a different runtime startup",
                        request_id=request_id,
                    )
                return runtime_startup_from_row(existing)
            connection.execute(
                """
                INSERT INTO runtime_startups (
                    request_id, workspace_session_id, tool_kind,
                    scratch_relative_path, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    workspace_session_id,
                    create["tool_kind"],
                    scratch_relative_path,
                    timestamp,
                    timestamp,
                ),
            )
            row = connection.execute(
                "SELECT * FROM runtime_startups WHERE request_id = ?",
                (request_id,),
            ).fetchone()
        return runtime_startup_from_row(row)

    def get_runtime_startup(
        self,
        request_id: str,
    ) -> RuntimeStartupRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM runtime_startups WHERE request_id = ?",
                (request_id,),
            ).fetchone()
        return None if row is None else runtime_startup_from_row(row)

    def update_runtime_startup_processes(
        self,
        request_id: str,
        *,
        worker_process_id: int | None,
        process_id: int | None,
        worker_process_identity: str | None = None,
        process_identity: str | None = None,
        now: str | None = None,
    ) -> RuntimeStartupRecord | None:
        timestamp = now or utc_now()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM runtime_startups WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None:
                return None
            connection.execute(
                """
                UPDATE runtime_startups
                SET worker_process_id = COALESCE(?, worker_process_id),
                    worker_process_identity = COALESCE(?, worker_process_identity),
                    process_id = COALESCE(?, process_id),
                    process_identity = COALESCE(?, process_identity),
                    updated_at = ?
                WHERE request_id = ?
                """,
                (
                    worker_process_id,
                    worker_process_identity,
                    process_id,
                    process_identity,
                    timestamp,
                    request_id,
                ),
            )
            result = connection.execute(
                "SELECT * FROM runtime_startups WHERE request_id = ?",
                (request_id,),
            ).fetchone()
        return runtime_startup_from_row(result)

    def mark_runtime_startup_spawn_attempted(
        self,
        request_id: str,
        *,
        now: str | None = None,
    ) -> RuntimeStartupRecord:
        """Persist the unsafe-to-assume-empty boundary before OS process spawn."""

        timestamp = now or utc_now()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM runtime_startups WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None:
                raise InteractiveInvalidRequestError(
                    "runtime startup ownership is missing",
                    request_id=request_id,
                )
            connection.execute(
                """
                UPDATE runtime_startups
                SET spawn_attempted = 1, updated_at = ?
                WHERE request_id = ?
                """,
                (timestamp, request_id),
            )
            result = connection.execute(
                "SELECT * FROM runtime_startups WHERE request_id = ?",
                (request_id,),
            ).fetchone()
        return runtime_startup_from_row(result)

    def clear_runtime_startup(self, request_id: str) -> bool:
        with self._transaction() as connection:
            cursor = connection.execute(
                "DELETE FROM runtime_startups WHERE request_id = ?",
                (request_id,),
            )
        return bool(cursor.rowcount)

    def list_runtime_startups(self) -> tuple[RuntimeStartupRecord, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM runtime_startups ORDER BY created_at, request_id"
            ).fetchall()
        return tuple(runtime_startup_from_row(row) for row in rows)
